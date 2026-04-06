import os
import json
import re
import polars as pl
import gspread
from google.oauth2.credentials import Credentials
from datetime import datetime
import traceback
import time

### NOTA: Algunos IDs carpetas son de 2025, revisar y corregir

# ==============================================================================
# 1. CONFIGURACIÓN DE IDs (Origen y Destino)
# ==============================================================================
PROJECTS_URL = "https://docs.google.com/spreadsheets/d/1Rx6e85e0vmLAF2SzOEnCl3k3C6VcYG_VRoBpyHxhqqw/edit?gid=0#gid=0"
BUSINESS_FOLDER_ID = "1DC1RBUERYgg-C0wXbKNdbfC8bDVcLf-g"
DEV_FOLDER_ID = "1xYvNKtNS_IDbF1TEW6kUZ3U9LyOSd4Yv"

PARENT_FOLDER_ID = "1E5SvDII6IBGDSqcH3bc2sjNGsu_dLCXG"

ALERTS_FOLDER_ID = "1QF9d74Svyjli0tb0EU492AtCZj0979oC"
CONSOLIDATED_FOLDER_ID = "1PdRIlCTiwZDxnjUiYXOpklbqWcJgEt5N"
DEST_DEV_FOLDER_ID = "1T6M1yt8dXexoeXpqd0-mnCU5VhmeHuKV"
DEST_BUSINESS_FOLDER_ID = "1p257669lmJv9uc4KVI5leiTlvp_2-eiB"

REVIEW_DATE = "2025-02-28"

# ==============================================================================
# 2. AUTENTICACIÓN Y ESCUDO ANTI-BLOQUEOS
# ==============================================================================
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    token_str = os.environ.get('GOOGLE_OAUTH_TOKEN')
    
    if token_str:
        print("🔑 Modo Nube: Usando token...")
        token_info = json.loads(token_str)
        creds = Credentials.from_authorized_user_info(token_info, scopes)
    else:
        print("🔑 Modo Local: Usando archivo token.json...")
        creds = Credentials.from_authorized_user_file('token.json', scopes)
        
    return gspread.authorize(creds)

def api_retry(max_retries=6):
    """Decorador: Si Google nos bloquea por cuota (429), pausa y reintenta automáticamente."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    if '429' in str(e) or '500' in str(e) or '503' in str(e):
                        wait_time = (attempt + 1) * 20  # Espera 20s, 40s, 60s...
                        print(f"    ⚠️ Límite de cuota de la API de Gdrive alcanzado. Respirando {wait_time}s antes de reintentar (Intento {attempt+1}/{max_retries})...")
                        time.sleep(wait_time)
                    else:
                        raise e
            raise Exception("Se superó el límite máximo de reintentos de la API.")
        return wrapper
    return decorator

# ==============================================================================
# 3. FUNCIONES CORE DE DATOS (CON REINTENTOS)
# ==============================================================================
def safe_read_sheet(sheet, range_name=None) -> pl.DataFrame:
    """Lee un Google Sheet protegiendo contra columnas duplicadas."""
    if range_name:
        raw_data = sheet.get(range_name)
    else:
        raw_data = sheet.get_all_values()
        
    if not raw_data or len(raw_data) < 2:
        return None
        
    headers = raw_data[0]
    clean_headers = []
    seen = {}
    
    for col in headers:
        col_name = col.strip() if col.strip() else "Unnamed"
        if col_name in seen:
            seen[col_name] += 1
            clean_headers.append(f"{col_name}_{seen[col_name]}")
        else:
            seen[col_name] = 0
            clean_headers.append(col_name)
            
    num_cols = len(clean_headers)
    data_rows = []
    for row in raw_data[1:]:
        padded_row = row + [""] * (num_cols - len(row))
        data_rows.append(padded_row[:num_cols])
        
    df = pl.DataFrame(data_rows, schema=clean_headers, orient="row")
    return df.with_columns(pl.all().cast(pl.Utf8))

@api_retry()
def get_sheet_data_safely(gc, file_id):
    """Abre y lee un archivo protegido por el escudo anti-bloqueos."""
    sheet = gc.open_by_key(file_id).worksheet("Timesheet")
    return safe_read_sheet(sheet, range_name="A2:I")

@api_retry()
def export_to_drive(gc, df: pl.DataFrame, file_name: str, folder_id: str):
    """Exporta datos protegido por el escudo anti-bloqueos, manteniendo tipos de datos."""
    if df.is_empty():
        return

    if folder_id in [BUSINESS_FOLDER_ID, DEV_FOLDER_ID]:
        print(f"🚨 ALERTA DE SEGURIDAD: Intento de escritura en carpeta protegida.")
        return

    # 1. Convertimos solo las columnas de Fecha a texto (para que la API no falle)
    columnas_fecha = [col for col in df.columns if df[col].dtype in [pl.Date, pl.Datetime]]
    if columnas_fecha:
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in columnas_fecha])

    # 2. Convertimos a lista de listas y cambiamos los "Nulos" por espacios en blanco ""
    datos_exportar = [list(df.columns)]
    for row in df.rows():
        fila_limpia = ["" if val is None else val for val in row]
        datos_exportar.append(fila_limpia)
    
    # 3. Buscar o crear el archivo
    files = gc.list_spreadsheet_files(folder_id=folder_id)
    file_id = next((f['id'] for f in files if f['name'] == file_name), None)
    
    if file_id:
        sh = gc.open_by_key(file_id)
    else:
        sh = gc.create(file_name, folder_id=folder_id)
    
    try:
        ws = sh.worksheet("Datos")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.sheet1
        ws.update_title("Datos")
        
    ws.clear()
    
    # 4. Magia aquí: "USER_ENTERED" obliga a Sheets a interpretar números y fechas correctamente
    ws.update(datos_exportar, value_input_option="USER_ENTERED")
    time.sleep(1) # Pequeña pausa de cortesía :v


def process_timesheets(gc, folder_id: str, sector_label: str, name_filter: str = None) -> pl.DataFrame:
    print(f"\n📂 Buscando timesheets en carpeta: {sector_label}...")
    
    files = gc.list_spreadsheet_files(folder_id=folder_id)
    
    # Si le damos un filtro, descarta todo lo que no coincida
    if name_filter:
        files = [f for f in files if f['name'].startswith(name_filter)]
        print(f"  🔍 Filtro aplicado: Encontrados {len(files)} archivos que empiezan con '{name_filter}'")
        
        # 👇 NUEVO: Imprimimos la lista de los archivos que SÍ pasaron el filtro
        print("  📑 Archivos a procesar:")
        for f in files:
            # 👇 NUEVO: Ahora imprimimos el nombre Y el ID del archivo
            print(f"     - {f['name']} | ID: {f['id']}")
        
    all_dfs = []  
    
    if not files:
        print(f"  ⚠️ La carpeta está vacía o ningún archivo coincide con el filtro.")
        return None

    for f in files:
        try:
            # Usamos la nueva función con reintentos
            df = get_sheet_data_safely(gc, f['id'])
            
            if df is not None:
                df = df.with_columns([
                    pl.lit(f['name']).alias("archivo_origen"),
                    pl.lit(sector_label).alias("Sector_Origen")
                ])
                all_dfs.append(df)
        except Exception as e:
            print (f"❌ Ignorado: '{f['name']}'. Motivo: {e}")
            
        time.sleep(1) # Ritmo base para evitar ahogar la API
            
    if not all_dfs:
        return None

    full_df = pl.concat(all_dfs, how="diagonal")

    if "...1" in full_df.columns:
        full_df = full_df.rename({"...1": "consecutivo"})
        
    # 1. Agregamos un batallón de formatos de fecha para que no se pierda ningún registro válido
    date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d.%m.%Y", "%d/%m/%y"]
    date_expressions = [pl.col("Fecha").str.strptime(pl.Date, format=fmt, strict=False) for fmt in date_formats]

    full_df = full_df.with_columns([
        pl.col("archivo_origen").str.split("-").list.last().str.strip_chars().alias("nombre"),
        pl.coalesce(date_expressions).alias("Fecha_dt")
    ])

    if "Cantidad de horas" in full_df.columns:
        full_df = full_df.with_columns(
            pl.col("Cantidad de horas").str.replace(",", ".").cast(pl.Float64, strict=False)
        )

    full_df = full_df.with_columns(pl.col("Fecha_dt").alias("Fecha")).drop("Fecha_dt")
    
    # 👇 NUEVO FILTRO: Conserva la fila si el usuario llenó AL MENOS UNA de estas columnas clave.
    # Así permitimos que pasen las filas con errores para que la "Lupa" genere las alertas después.
    full_df = full_df.filter(
        pl.col("Cantidad de horas").is_not_null() | 
        (pl.col("Nombre de Proyecto").is_not_null() & (pl.col("Nombre de Proyecto").cast(pl.Utf8).str.strip_chars() != "")) |
        (pl.col("Descripción").is_not_null() & (pl.col("Descripción").cast(pl.Utf8).str.strip_chars() != "")) |
        (pl.col("Category").is_not_null() & (pl.col("Category").cast(pl.Utf8).str.strip_chars() != ""))
    )

    print(f"  ✅ {sector_label}: Procesados {len(full_df)} registros válidos.")
    return full_df

# ==============================================================================
# 4. PIPELINE PRINCIPAL
# ==============================================================================
def run_pipeline():
    print("🚀 Iniciando Pipeline PMO...\n")
    try:
        gc = get_gspread_client()
        
        print("📚 Importando maestro de proyectos...")
        projects_sheet = gc.open_by_url(PROJECTS_URL).worksheet("Proyectos")
        projects_df = safe_read_sheet(projects_sheet)
        
        if projects_df is None:
            return
            
        clean_projects_df = projects_df.select([
            pl.col("Tipo de proyecto").alias("sector"),
            pl.col("Nombre de Proyecto").alias("proyecto"),
            pl.col("Activo")
        ])
        valid_projects_list = clean_projects_df.get_column("proyecto").drop_nulls().to_list()

        # Le aplicamos el filtro exacto a TODAS las carpetas
        timesheet_filter = "Timesheet 2026 -"
        
        business_df = process_timesheets(gc, BUSINESS_FOLDER_ID, "Empresarial", name_filter=timesheet_filter)
        dev_df = process_timesheets(gc, DEV_FOLDER_ID, "Desarrollo", name_filter=timesheet_filter)
        
        # Aquí procesa la carpeta padre (los sueltos)
        folder_father_df = process_timesheets(gc, PARENT_FOLDER_ID, "Directorio", name_filter=timesheet_filter)

        # 👇 EL PASO CRÍTICO: Asegúrate de que 'directorio_df' esté dentro de estos corchetes cuadrados. 
        # Si no está ahí, Python lo extrae pero luego lo bota a la basura en vez de pegarlo.
        dfs_to_combine = [df for df in [business_df, dev_df, folder_father_df] if df is not None]

        if not dfs_to_combine:
            print("❌ No se encontró ningún dato en ninguna carpeta.")
            return
            
        print("\n⚡ Consolidando datos...")
        consolidated_df = pl.concat(dfs_to_combine, how="diagonal")

        # 👇 NUEVO: Bautizamos "Unnamed" a "consecutivo" para TODOS los reportes de una vez
        if "Unnamed" in consolidated_df.columns:
            consolidated_df = consolidated_df.rename({"Unnamed": "consecutivo"})

        print("🚨 Generando alertas...")
        limit_date = datetime.strptime(REVIEW_DATE, "%Y-%m-%d").date()
        
        category_cond = pl.col("Category").is_null() | (pl.col("Category") == "")
        subcat_cond = pl.col("Sub-Category").is_null() | (pl.col("Sub-Category") == "")
        projects_cond = (pl.col("Category") == "Proyectos") & (pl.col("Nombre de Proyecto").is_null() | (pl.col("Nombre de Proyecto") == ""))
        hours_cond = pl.col("Cantidad de horas").is_null()
        desc_cond = pl.col("Descripción").is_null() | (pl.col("Descripción") == "")
        project_name_cond = (
            pl.col("Nombre de Proyecto").is_not_null() & 
            (pl.col("Nombre de Proyecto") != "") & 
            (pl.col("Nombre de Proyecto") != "Otro") & 
            ~pl.col("Nombre de Proyecto").is_in(valid_projects_list)
        )

        base_alerts_df = consolidated_df.with_columns([
            category_cond.cast(pl.Int32).alias("alerta_category"),
            subcat_cond.cast(pl.Int32).alias("alerta_subcat"),
            projects_cond.cast(pl.Int32).alias("alerta_proyectos"),
            hours_cond.cast(pl.Int32).alias("alerta_horas"),
            desc_cond.cast(pl.Int32).alias("alerta_desc"),
            project_name_cond.cast(pl.Int32).alias("alerta_proyecto_nomb")
        ])

        alert_cols = ["alerta_category", "alerta_subcat", "alerta_proyectos", "alerta_horas", "alerta_desc", "alerta_proyecto_nomb"]
        base_alerts_df = base_alerts_df.with_columns(pl.sum_horizontal(alert_cols).alias("suma_alertas"))

        alerts_detail_df = base_alerts_df.filter(pl.col("suma_alertas") > 0).sort("nombre")
        
        alerts_summary_df = base_alerts_df.group_by("nombre").agg([
            *[pl.col(c).sum() for c in alert_cols],
            pl.col("Fecha").max().alias("max_fecha")
        ]).with_columns([
            (pl.col("max_fecha") < limit_date).cast(pl.Int32).alias("alerta_fecha")
        ]).with_columns(
            (pl.sum_horizontal(alert_cols) + pl.col("alerta_fecha")).alias("suma_alertas")
        ).filter(pl.col("suma_alertas") >= 1).sort("suma_alertas", descending=True)
        
        #Por si queremos reestructurar el orden de las columnas en el archivo de salida
        ### Ordenar columnas para el resumen de alertas

        alerts_summary_df = alerts_summary_df.select([
            "nombre",
            "alerta_category",
            "alerta_subcat",
            "alerta_proyectos",
            "alerta_horas",
            "alerta_desc",
            "alerta_fecha",
            "alerta_proyecto_nomb",
            "max_fecha",
            "suma_alertas"
        ])

        ### Ordenar columnas para el detalle de alertas
        
        alerts_detail_df = alerts_detail_df.select([
            "archivo_origen",
            "nombre",
            "consecutivo",
            "Fecha",
            "Día",
            "Category",
            "Sub-Category",
            "Nombre de Proyecto",
            "Descripción",
            "Cantidad de horas",
            "Mes",
            "alerta_category",
            "alerta_subcat",
            "alerta_proyectos",
            "alerta_horas",
            "alerta_desc",
            "alerta_proyecto_nomb",
            "suma_alertas",
        ])
        
        print("  📤 Exportando alertas...")
        export_to_drive(gc, alerts_summary_df, "Resumen Alertas", ALERTS_FOLDER_ID)
        export_to_drive(gc, alerts_detail_df, "Detalle Alertas", ALERTS_FOLDER_ID)

        # 2. Columnas formato original
        orden_r = [
            "archivo_origen",
            "nombre",
            "consecutivo",
            "Fecha",
            "Día",
            "Category",
            "Sub-Category",
            "Nombre de Proyecto",
            "Descripción",
            "Cantidad de horas",
            "Mes"
        ]
        
        # Nueva columna
        if "Sector_Origen" in consolidated_df.columns:
            orden_r.append("Sector_Origen")

        # 3. Aplicamos el orden
        columnas_finales = [col for col in orden_r if col in consolidated_df.columns]
        consolidated_df = consolidated_df.select(columnas_finales)


        print("📤 Exportando Consolidado General...")
        export_to_drive(gc, consolidated_df, "Productividad Equi Consolidado", CONSOLIDATED_FOLDER_ID)
        
    except Exception as e:
        print("\n❌ El pipeline falló. Detalle del error:")
        traceback.print_exc()

if __name__ == "__main__":
    run_pipeline()