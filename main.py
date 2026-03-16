import polars as pl
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time

# --- CONFIGURACIÓN DE IDs ---
MASTER_SHEET_ID = "1vagciENUogP8mGLdBmaMN6BlvUqp9VRuruyI35usJJg" # Master Timesheets (dummy) creado con copias de los timesheets individuales revisar el año porque no me acuerdo
PROJECT_MASTER_ID = "1iokanQGw0Vtag6PKVWPCNY5kWHNKQ1qA7djVlHG7Wdc" #Es una copia del original está en Mi Unidad
CREDENTIALS_FILE = "credentials.json"

# IDs de las carpetas de destino
ALERTS_FOLDER_ID = "1Mq_6p2w5LXbBLzgBZ0aoqSt_wcFBCSpj"
DES_FOLDER_ID = "1jZmIClm8dArKvGvyX8RtKADHoObM9obq"
EMP_FOLDER_ID = "1kiGp_zlBWyiD7CEVr1QkgzuK_cj7QuE7"

def get_google_client():
    # Configuración de autenticación con Google Drive y Sheets
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)

def export_to_drive(gc, df, file_name, folder_id):
    """Función corregida para crear o actualizar archivos en carpetas específicas."""
    try:
        # 1. Buscamos el archivo por nombre
        # Nota: gspread busca en todos los archivos a los que tiene acceso el bot
        spreadsheet = None
        try:
            # Intentamos abrir el archivo por su nombre exacto
            spreadsheet = gc.open(file_name)
            print(f"   🔄 Actualizando: {file_name}")
        except gspread.exceptions.SpreadsheetNotFound:
            # 2. Si no existe, lo creamos dentro de la carpeta específica
            spreadsheet = gc.create(file_name, folder_id)
            print(f"   🆕 Creado nuevo: {file_name}")
        
        # 3. Accedemos a la primera pestaña y actualizamos
        worksheet = spreadsheet.get_worksheet(0)
        worksheet.clear()
        
        # Convertimos DataFrame a lista para subir (incluyendo encabezados)
        data_to_upload = [df.columns] + df.to_numpy().tolist()
        worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
        
    except Exception as e:
        print(f"      ❌ Error exportando {file_name}: {e}")

def run_pipeline():
    print(f"🚀 Iniciando Pipeline PMO...")
    gc = get_google_client()
    
   # --- 1. CARGAR MÁSTER DE PROYECTOS (Validación) ---
    print("📋 Cargando maestra de proyectos para validación...")
    project_sh = gc.open_by_key(PROJECT_MASTER_ID)
    
    # 1.1. Usamos get_all_values() para evitar el error de duplicados
    raw_project_data = project_sh.worksheet("Proyectos").get_all_values()
    
    if len(raw_project_data) > 1:
        # Extraemos encabezados y datos por separado
        headers_raw = raw_project_data[0]
        rows_data = raw_project_data[1:]
        
        # 1.2. Hacemos que los encabezados sean únicos "on the fly"
        project_headers = []
        for i, h in enumerate(headers_raw):
            clean_h = h.strip()
            if not clean_h or clean_h in project_headers:
                project_headers.append(f"{clean_h}_{i}" if clean_h else f"Col_{i}")
            else:
                project_headers.append(clean_h)
        
        # 1.3. Creamos el DataFrame de Polars
        df_projects_ref = pl.DataFrame(rows_data, schema=project_headers)
        
        # 1.4. Seleccionamos las columnas que nos interesan (nombres exactos de tu Maestra)
        df_projects_ref = df_projects_ref.select([
            pl.col("Tipo de proyecto").alias("sector"),
            pl.col("Nombre de Proyecto").alias("project_name_ref"),
            pl.col("Activo")
        ])
    
    # Lista de proyectos válidos para el filtro de alertas
    valid_projects_list = df_projects_ref.filter(pl.col("Activo") == "Activo")["project_name_ref"].to_list()

    # --- 2. EXTRACCIÓN Y LIMPIEZA DE TIMESHEETS ---
    all_files = gc.list_spreadsheet_files() 
    dfs_collection = []
    
    for file_info in all_files:
        name = file_info['name']
        file_id = file_info['id']
        
        # Ignorar el Master y otros archivos consolidados
        if file_id == MASTER_SHEET_ID or "CONSOLIDADO" in name.upper(): 
            continue
            
        try:
            print(f"   ⏳ Procesando: {name}...")
            time.sleep(1.5) # Evitar saturación de la API
            
            ss = gc.open_by_key(file_id)
            target_ws = next((ws for ws in ss.worksheets() if "TIMESHEET" in ws.title.upper()), None)
            
            if target_ws:
                raw_data = target_ws.get_all_values()
                if len(raw_data) >= 2:
                    headers = raw_data[1][:9] # Columnas A-I
                    rows = [row[:9] for row in raw_data[2:]]
                    
                    if rows:
                        temp_df = pl.DataFrame(rows, schema=headers, orient="row")
                        
                        # Limpieza de nombres de archivo y persona
                        name_parts = name.split(" - ")
                        person_name = name_parts[-1] if len(name_parts) > 1 else "Unknown"
                        base_file_name = name_parts[0].replace("Copia de ", "")
                        
                        # Estandarizar columna consecutivo
                        first_col_name = temp_df.columns[0]
                        temp_df = temp_df.rename({first_col_name: "consecutivo"})
                        
                        # Añadir columnas de trazabilidad (Estructura A-K)
                        temp_df = temp_df.with_columns([
                            pl.lit(base_file_name).alias("archivo_origen"),
                            pl.lit(person_name).alias("nombre")
                        ])
                        
                        # Reordenar columnas exacto como requiere el reporte
                        column_order = ["archivo_origen", "nombre", "consecutivo", "Fecha", "Día", 
                                       "Category", "Sub-Category", "Nombre de Proyecto", "Descripción", 
                                       "Cantidad de horas", "Mes"]
                        temp_df = temp_df.select(column_order)

                        # Filtros de limpieza básicos
                        temp_df = temp_df.filter(
                            (~pl.col("Fecha").str.contains("1899")) & 
                            (pl.col("Category") != "") & 
                            (pl.col("Cantidad de horas") != "") &
                            (pl.col("Cantidad de horas") != "0")
                        )
                        
                        if temp_df.height > 0:
                            dfs_collection.append(temp_df)
                            print(f"      ✅ {temp_df.height} filas extraídas.")
                
        except Exception as e:
            print(f"      ❌ Error en {name}: {e}")

    if not dfs_collection: 
        print("Empty collection. Check source files.")
        return
        
    final_df = pl.concat(dfs_collection, how="diagonal")

    # --- 3. LÓGICA DE ALERTAS (Replicando auditoría de R) ---
    print("🚨 Ejecutando auditoría de datos (Alertas)...")
    detail_alerts = final_df.with_columns([
        pl.when(pl.col("Category") == "").then(1).otherwise(0).alias("al_cat"),
        pl.when(pl.col("Sub-Category") == "").then(1).otherwise(0).alias("al_sub"),
        pl.when((pl.col("Category") == "Proyectos") & (pl.col("Nombre de Proyecto") == "")).then(1).otherwise(0).alias("al_proy_missing"),
        pl.when(pl.col("Descripción") == "").then(1).otherwise(0).alias("al_desc"),
        pl.when((pl.col("Nombre de Proyecto") != "") & 
                (pl.col("Nombre de Proyecto") != "Otro") & 
                (~pl.col("Nombre de Proyecto").is_in(valid_projects_list))).then(1).otherwise(0).alias("al_invalid_proy")
    ])
    
    # Calcular suma de alertas por fila
    detail_alerts = detail_alerts.with_columns([
        (pl.col("al_cat") + pl.col("al_sub") + pl.col("al_proy_missing") + 
         pl.col("al_desc") + pl.col("al_invalid_proy")).alias("suma_alertas")
    ]).filter(pl.col("suma_alertas") > 0)

    # Resumen de alertas por persona
    summary_alerts = detail_alerts.group_by("nombre").agg([
        pl.col("suma_alertas").sum().alias("Total Alertas"),
        pl.col("Fecha").max().alias("Última Fecha Reportada")
    ]).sort("Total Alertas", descending=True)

    # Exportar reportes de alertas
    export_to_drive(gc, summary_alerts, "Resumen Alertas", ALERTS_FOLDER_ID)
    export_to_drive(gc, detail_alerts, "Detalle Alertas", ALERTS_FOLDER_ID)

    # --- 4. ACTUALIZAR MASTER SHEET ---
    print("💾 Actualizando Master 'Productividad Equi Consolidado'...")
    master_ss = gc.open_by_key(MASTER_SHEET_ID)
    master_ws = master_ss.get_worksheet(0)
    master_ws.clear()
    master_data = [final_df.columns] + final_df.to_numpy().tolist()
    master_ws.update(master_data, value_input_option='USER_ENTERED')

    # --- 5. PARTICIÓN POR SECTOR Y PROYECTO ---
    print("📂 Distribuyendo datos por sector y proyecto...")
    # Unir con maestra para obtener el Sector (Desarrollo/Empresarial)
    df_with_sectors = final_df.join(df_projects_ref, left_on="Nombre de Proyecto", right_on="project_name_ref", how="left")
    
    for sector_name in ["Desarrollo", "Empresarial"]:
        current_folder_id = DES_FOLDER_ID if sector_name == "Desarrollo" else EMP_FOLDER_ID
        sector_filtered_df = df_with_sectors.filter(pl.col("sector") == sector_name)
        
        unique_projects = sector_filtered_df["Nombre de Proyecto"].unique().to_list()
        for project in unique_projects:
            if project:
                project_df = sector_filtered_df.filter(pl.col("Nombre de Proyecto") == project)
                # Formatear nombre de archivo: Espacios por guiones bajos
                clean_file_name = f"{project.replace(' ', '_')}_productividad"
                export_to_drive(gc, project_df, clean_file_name, current_folder_id)

    print("-" * 45 + "\n🎊 PROCESO COMPLETADO EXITOSAMENTE \n" + "-" * 45)

if __name__ == "__main__":
    run_pipeline()