import polars as pl
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time

# --- CONFIGURATION ---
MASTER_SHEET_ID = "1vagciENUogP8mGLdBmaMN6BlvUqp9VRuruyI35usJJg" 
PROJECT_MASTER_ID = "1iokanQGw0Vtag6PKVWPCNY5kWHNKQ1qA7djVlHG7Wdc"
CREDENTIALS_FILE = "credentials.json"

# IDs de las carpetas dentro de la Unidad Compartida
ALERTS_FOLDER_ID = "1Mq_6p2w5LXbBLzgBZ0aoqSt_wcFBCSpj"
DES_FOLDER_ID = "1jZmIClm8dArKvGvyX8RtKADHoObM9obq"
EMP_FOLDER_ID = "1kiGp_zlBWyiD7CEVr1QkgzuK_cj7QuE7"

def get_google_client():
    # Autenticación mediante Service Account (Sin usar OAuth personal)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)

def export_to_drive(gc, df, file_name, folder_id):
    # Función robusta para crear o actualizar archivos en Unidades Compartidas
    try:
        spreadsheet = None
        # 1. Intentamos buscar si el archivo ya existe por nombre
        # list_spreadsheet_files devuelve todos los archivos accesibles por el bot
        all_files = gc.list_spreadsheet_files()
        
        # Buscamos el ID del archivo que coincida con el nombre
        existing_file = next((f for f in all_files if f['name'] == file_name), None)
        
        if existing_file:
            spreadsheet = gc.open_by_key(existing_file['id'])
            print(f"   🔄 Actualizando: {file_name}")
        else:
            # 2. Si no existe, lo creamos en la carpeta de la Unidad Compartida
            spreadsheet = gc.create(file_name, folder_id)
            print(f"   🆕 Creado nuevo en Unidad Compartida: {file_name}")
        
        # 3. Limpiar y actualizar datos
        worksheet = spreadsheet.get_worksheet(0)
        worksheet.clear()
        
        # Preparamos datos para subir con formato correcto
        data_to_upload = [df.columns] + df.to_numpy().tolist()
        worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
        
    except Exception as e:
        print(f"      ❌ Error exportando {file_name}: {e}")

def run_pipeline():
    print(f"🚀 Iniciando Pipeline PMO en Unidad Compartida...")
    gc = get_google_client()
    
    # --- 1. CARGAR MAESTRA DE PROYECTOS ---
    print("📋 Cargando máster de proyectos...")
    project_sh = gc.open_by_key(PROJECT_MASTER_ID)
    raw_project_data = project_sh.worksheet("Proyectos").get_all_values()

    if len(raw_project_data) > 1:
        headers_raw = raw_project_data[0]
        rows_data = raw_project_data[1:]
        
        # Limpieza de encabezados duplicados (Evita el error de 'Días transcurridos')
        project_headers = []
        for i, h in enumerate(headers_raw):
            clean_h = h.strip()
            if not clean_h or clean_h in project_headers:
                project_headers.append(f"{clean_h}_{i}" if clean_h else f"Col_{i}")
            else:
                project_headers.append(clean_h)
        
        # Creamos DataFrame de validación (orient="row" quita el aviso amarillo)
        df_projects_ref = pl.DataFrame(rows_data, schema=project_headers, orient="row").select([
            pl.col("Tipo de proyecto").alias("sector"),
            pl.col("Nombre de Proyecto").alias("project_name_ref"),
            pl.col("Activo")
        ])
        valid_projects_list = df_projects_ref.filter(pl.col("Activo") == "Activo")["project_name_ref"].to_list()

    # --- 2. EXTRACCIÓN (Lógica de la semana pasada) ---
    all_drive_files = gc.list_spreadsheet_files() 
    dfs_collection = []
    
    for file_info in all_drive_files:
        name = file_info['name']
        file_id = file_info['id']
        
        if file_id == MASTER_SHEET_ID or "CONSOLIDADO" in name.upper(): 
            continue
            
        try:
            print(f"   ⏳ Procesando: {name}...")
            time.sleep(1.2) # Pausa para evitar error 429
            
            ss = gc.open_by_key(file_id)
            target_ws = next((ws for ws in ss.worksheets() if "TIMESHEET" in ws.title.upper()), None)
            
            if target_ws:
                raw_data = target_ws.get_all_values()
                if len(raw_data) >= 2:
                    headers = raw_data[1][:9]
                    rows = [row[:9] for row in raw_data[2:]]
                    
                    if rows:
                        temp_df = pl.DataFrame(rows, schema=headers, orient="row")
                        
                        # Parsing de nombres (Igual que en R)
                        name_parts = name.split(" - ")
                        person_name = name_parts[-1] if len(name_parts) > 1 else "Unknown"
                        base_file_name = name_parts[0].replace("Copia de ", "")
                        
                        temp_df = temp_df.rename({temp_df.columns[0]: "consecutivo"})
                        temp_df = temp_df.with_columns([
                            pl.lit(base_file_name).alias("archivo_origen"),
                            pl.lit(person_name).alias("nombre")
                        ])
                        
                        column_order = ["archivo_origen", "nombre", "consecutivo", "Fecha", "Día", 
                                       "Category", "Sub-Category", "Nombre de Proyecto", "Descripción", 
                                       "Cantidad de horas", "Mes"]
                        temp_df = temp_df.select(column_order)

                        # Filtros de limpieza
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

    if not dfs_collection: return
    final_df = pl.concat(dfs_collection, how="diagonal")

    # --- 3. ALERTAS Y AUDITORÍA ---
    print("🚨 Ejecutando auditoría (Alertas)...")
    detail_alerts = final_df.with_columns([
        pl.when(pl.col("Category") == "").then(1).otherwise(0).alias("al_cat"),
        pl.when(pl.col("Sub-Category") == "").then(1).otherwise(0).alias("al_sub"),
        pl.when((pl.col("Category") == "Proyectos") & (pl.col("Nombre de Proyecto") == "")).then(1).otherwise(0).alias("al_proy_missing"),
        pl.when(pl.col("Descripción") == "").then(1).otherwise(0).alias("al_desc"),
        pl.when((pl.col("Nombre de Proyecto") != "") & 
                (pl.col("Nombre de Proyecto") != "Otro") & 
                (~pl.col("Nombre de Proyecto").is_in(valid_projects_list))).then(1).otherwise(0).alias("al_invalid_proy")
    ])
    
    detail_alerts = detail_alerts.with_columns([
        (pl.col("al_cat") + pl.col("al_sub") + pl.col("al_proy_missing") + 
         pl.col("al_desc") + pl.col("al_invalid_proy")).alias("suma_alertas")
    ]).filter(pl.col("suma_alertas") > 0)

    summary_alerts = detail_alerts.group_by("nombre").agg([
        pl.col("suma_alertas").sum().alias("Total Alertas"),
        pl.col("Fecha").max().alias("Ultimo Reporte")
    ]).sort("Total Alertas", descending=True)

    # Exportar alertas (Aquí ya no debería dar error 403)
    export_to_drive(gc, summary_alerts, "Resumen Alertas", ALERTS_FOLDER_ID)
    export_to_drive(gc, detail_alerts, "Detalle Alertas", ALERTS_FOLDER_ID)

    # --- 4. MASTER CONSOLIDADO ---
    print("💾 Actualizando Master Consolidado...")
    export_to_drive(gc, final_df, "Productividad Equi Consolidado", "1wJgmrxlxvTcC-kbTUasTO8hOkh0Wg2GK")

    # --- 5. SEPARACIÓN POR SECTOR ---
    print("📂 Distribuyendo por Proyectos...")
    df_with_sectors = final_df.join(df_projects_ref, left_on="Nombre de Proyecto", right_on="project_name_ref", how="left")
    
    for sector_name in ["Desarrollo", "Empresarial"]:
        folder_id = DES_FOLDER_ID if sector_name == "Desarrollo" else EMP_FOLDER_ID
        sector_data = df_with_sectors.filter(pl.col("sector") == sector_name)
        
        for project in sector_data["Nombre de Proyecto"].unique().to_list():
            if project:
                project_df = sector_data.filter(pl.col("Nombre de Proyecto") == project)
                clean_name = f"{project.replace(' ', '_')}_productividad"
                export_to_drive(gc, project_df, clean_name, folder_id)

    print("-" * 45 + "\n🎊 PROCESO COMPLETADO EXITOSAMENTE \n" + "-" * 45)

if __name__ == "__main__":
    run_pipeline()