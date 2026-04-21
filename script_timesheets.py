import os
import sys # <-- AÑADIDO: Para fallar ruidosamente en GitHub
import json
import re
import polars as pl
import gspread
from google.oauth2.credentials import Credentials
from datetime import datetime
import traceback
import time

# ==============================================================================
# 1. CONFIGURACIÓN DE IDs (Origen y Destino)
# ==============================================================================
PROJECTS_URL = "https://docs.google.com/spreadsheets/d/1Rx6e85e0vmLAF2SzOEnCl3k3C6VcYG_VRoBpyHxhqqw/edit?gid=0#gid=0"
BUSINESS_FOLDER_ID = "1D4CQzLhIlr6iib3bCkV1juGi_Q5GNL1T"
DEV_FOLDER_ID = "1usvH3yjnXvWHUKLI0ZrFbRJJ3koaSZ1R"
PARENT_FOLDER_ID = "1fogkf7ANBU5BeFLUmdGmoh_W2vzQKqKZ"

ALERTS_FOLDER_ID = "1QF9d74Svyjli0tb0EU492AtCZj0979oC"
CONSOLIDATED_FOLDER_ID = "1PdRIlCTiwZDxnjUiYXOpklbqWcJgEt5N"
DEST_DEV_FOLDER_ID = "1T6M1yt8dXexoeXpqd0-mnCU5VhmeHuKV"
DEST_BUSINESS_FOLDER_ID = "1p257669lmJv9uc4KVI5leiTlvp_2-eiB"

REVIEW_DATE = "2025-02-28"

# ==============================================================================
# 2. AUTENTICACIÓN Y ESCUDO ANTI-BLOQUEOS
# ==============================================================================
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
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
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    if '429' in str(e) or '500' in str(e) or '503' in str(e):
                        wait_time = (attempt + 1) * 20
                        print(f"    ⚠️ Límite de cuota alcanzado. Respirando {wait_time}s antes de reintentar (Intento {attempt+1}/{max_retries})...")
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
    sheet = gc.open_by_key(file_id).worksheet("Timesheet")
    return safe_read_sheet(sheet, range_name="A2:I")

@api_retry()
def export_to_drive(gc, df: pl.DataFrame, file_name: str, folder_id: str):
    if df.is_empty():
        # <-- AÑADIDO: Alerta visible
        print(f"⚠️ ATENCIÓN: El reporte '{file_name}' está vacío. No se exportará.")
        return

    if folder_id in [BUSINESS_FOLDER_ID, DEV_FOLDER_ID]:
        print(f"🚨 ALERTA DE SEGURIDAD: Intento de escritura en carpeta protegida.")
        return

    columnas_fecha = [col for col in df.columns if df[col].dtype in [pl.Date, pl.Datetime]]
    if columnas_fecha:
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in columnas_fecha])

    datos_exportar = [list(df.columns)]
    for row in df.rows():
        fila_limpia = ["" if val is None else val for val in row]
        datos_exportar.append(fila_limpia)
    
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
    ws.update(datos_exportar, value_input_option="USER_ENTERED")
    time.sleep(1) 


def process_timesheets(gc, folder_id: str, sector_label: str, name_filter: str = None) -> pl.DataFrame:
    print(f"\n📂 Buscando timesheets en carpeta: {sector_label}...")
    files = gc.list_spreadsheet_files(folder_id=folder_id)
    
    if name_filter:
        files = [f for f in files if f['name'].startswith(name_filter) and "Plantilla" not in f['name']]
        print(f"  🔍 Filtro aplicado: Encontrados {len(files)} archivos.")
        for f in files:
            print(f"     - {f['name']} | ID: {f['id']}")
        
    all_dfs = []  
    if not files:
        print(f"  ⚠️ La carpeta está vacía o ningún archivo coincide con el filtro.")
        return None

    for f in files:
        max_intentos_archivo = 3  
        for intento in range(max_intentos_archivo):
            try:
                df = get_sheet_data_safely(gc, f['id'])
                if df is not None:
                    if "Nombre de Proyecto" in df.columns:
                        df = df.rename({"Nombre de Proyecto": "Proyecto"})
                    df = df.with_columns([
                        pl.lit(f['name']).alias("archivo_origen"),
                        pl.lit(sector_label).alias("Sector_Origen")
                    ])
                    all_dfs.append(df)
                break 
            except Exception as e:
                if intento < max_intentos_archivo - 1:
                    print(f"    ⚠️ Error de conexión con '{f['name']}'. Reintentando {intento + 1}/{max_intentos_archivo} en 5s...")
                    time.sleep(5)
                else:
                    print(f"❌ Ignorado definitivamente: '{f['name']}'. Motivo: {e}")
        time.sleep(1)
            
    if not all_dfs:
        return None

    full_df = pl.concat(all_dfs, how="diagonal")

    if "...1" in full_df.columns:
        full_df = full_df.rename({"...1": "consecutivo"})
        
    date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d.%m.%Y", "%d/%m/%y"]
    mapa_meses = {"1": "ene", "2": "feb", "3": "mar", "4": "abr", "5": "may", "6": "jun", "7": "jul", "8": "ago", "9": "sep", "10": "oct", "11": "nov", "12": "dic"}

    full_df = full_df.with_columns([
        pl.col("archivo_origen").str.split("-").list.last().str.strip_chars().alias("nombre"),
        pl.col("Proyecto").str.strip_chars(),
        pl.col("Proyecto").str.extract(r"^([\d-]+)", 1).str.replace_all("-", "_").alias("proyecto_id"),
        pl.col("Cantidad de horas").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.coalesce([
            pl.col("Fecha").cast(pl.Utf8).str.strip_chars().str.replace_all(r"\s+", "").str.strptime(pl.Date, format=fmt, strict=False) 
            for fmt in date_formats
        ]).alias("Fecha_Temporal")
    ])

    full_df = full_df.with_columns([
        pl.when(pl.col("Fecha_Temporal").is_not_null())
        .then(pl.col("Fecha_Temporal").dt.month().cast(pl.Utf8).replace(mapa_meses) + "/" + pl.col("Fecha_Temporal").dt.year().cast(pl.Utf8))
        .otherwise(pl.col("Mes"))
        .alias("Mes"),

        pl.when(pl.col("Fecha_Temporal").is_not_null())
        .then(pl.col("Fecha_Temporal").dt.strftime("%Y-%m-%d"))
        .otherwise(pl.col("Fecha"))
        .alias("Fecha")
    ]).drop("Fecha_Temporal")

    conteo_sucio = len(full_df)
    full_df = full_df.filter(
        pl.col("Cantidad de horas").is_not_null() | 
        (pl.col("Proyecto").is_not_null() & (pl.col("Proyecto").cast(pl.Utf8).str.strip_chars() != "")) |
        (pl.col("Descripción").is_not_null() & (pl.col("Descripción").cast(pl.Utf8).str.strip_chars() != "")) |
        (pl.col("Category").is_not_null() & (pl.col("Category").cast(pl.Utf8).str.strip_chars() != ""))
    )
    registros_finales = len(full_df)

    print(f"    📊 Resumen de limpieza {sector_label}:")
    print(f"      - Registros totales en archivos: {conteo_sucio}")
    print(f"      - Filas vacías eliminadas: {conteo_sucio - registros_finales}")
    print(f"      - ✅ REGISTROS VÁLIDOS: {registros_finales}")

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
            # <-- AÑADIDO: Si falla la lectura base, cancelamos todo con error
            print("❌ CRÍTICO: No se pudo leer el maestro de proyectos.")
            sys.exit(1)
            
        clean_projects_df = projects_df.select([
            pl.col("Tipo de proyecto").alias("sector"),
            pl.col("Nombre de Proyecto").alias("proyecto"),
            pl.col("Activo")
        ])
        valid_projects_list = clean_projects_df.get_column("proyecto").drop_nulls().to_list()

        timesheet_filter = "Timesheet 2026 -"
        
        business_df = process_timesheets(gc, BUSINESS_FOLDER_ID, "Empresarial", name_filter=timesheet_filter)
        dev_df = process_timesheets(gc, DEV_FOLDER_ID, "Desarrollo", name_filter=timesheet_filter)
        folder_father_df = process_timesheets(gc, PARENT_FOLDER_ID, "Directorio", name_filter=timesheet_filter)

        dfs_to_combine = [df for df in [business_df, dev_df, folder_father_df] if df is not None]

        if not dfs_to_combine:
            # <-- AÑADIDO: Error real para GitHub Actions
            print("❌ CRÍTICO: No se encontró ningún dato en ninguna carpeta para consolidar.")
            sys.exit(1)
            
        print("\n⚡ Consolidando datos...")
        consolidated_df = pl.concat(dfs_to_combine, how="diagonal")

        if "Unnamed" in consolidated_df.columns:
            consolidated_df = consolidated_df.rename({"Unnamed": "consecutivo"})

        print("🚨 Generando alertas...")
        limit_date = datetime.strptime(REVIEW_DATE, "%Y-%m-%d").date()
        
        category_cond = pl.col("Category").is_null() | (pl.col("Category") == "")
        subcat_cond = pl.col("Sub-Category").is_null() | (pl.col("Sub-Category") == "")
        projects_cond = (pl.col("Category") == "Proyectos") & (pl.col("Proyecto").is_null() | (pl.col("Proyecto") == ""))
        hours_cond = pl.col("Cantidad de horas").is_null()
        desc_cond = pl.col("Descripción").is_null() | (pl.col("Descripción") == "")

        project_name_cond = (
            pl.col("Proyecto").is_not_null() & 
            (pl.col("Proyecto") != "") & 
            (pl.col("Proyecto") != "Otro") & 
            ~pl.col("Proyecto").is_in(valid_projects_list)
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
            (pl.col("max_fecha").str.strptime(pl.Date, format="%Y-%m-%d", strict=False) < limit_date)
            .cast(pl.Int32).fill_null(0).alias("alerta_fecha")
        ]).with_columns(
            (pl.sum_horizontal(alert_cols) + pl.col("alerta_fecha")).alias("suma_alertas")
        ).filter(pl.col("suma_alertas") >= 1).sort("suma_alertas", descending=True)
        
        alerts_summary_df = alerts_summary_df.select([
            "nombre", "alerta_category", "alerta_subcat", "alerta_proyectos",
            "alerta_horas", "alerta_desc", "alerta_fecha", "alerta_proyecto_nomb",
            "max_fecha", "suma_alertas"
        ])

        alerts_detail_df = alerts_detail_df.select([
            "archivo_origen", "nombre", "consecutivo", "Fecha", "Día", "Category",
            "Sub-Category", "Proyecto", "Descripción", "Cantidad de horas", "Mes",
            "alerta_category", "alerta_subcat", "alerta_proyectos", "alerta_horas",
            "alerta_desc", "alerta_proyecto_nomb", "suma_alertas",
        ])
        
        print("  📤 Exportando alertas...")
        export_to_drive(gc, alerts_summary_df, "Resumen Alertas", ALERTS_FOLDER_ID)
        export_to_drive(gc, alerts_detail_df, "Detalle Alertas", ALERTS_FOLDER_ID)

        orden_r = [
            "archivo_origen", "nombre", "consecutivo", "Fecha", "Día", "Category",
            "Sub-Category", "Proyecto", "Descripción", "Cantidad de horas", "Mes"
        ]
        
        if "Sector_Origen" in consolidated_df.columns:
            orden_r.append("Sector_Origen")

        columnas_finales = [col for col in orden_r if col in consolidated_df.columns]
        consolidated_df = consolidated_df.select(columnas_finales)

        # =========================================================
        # 🌟 CREACIÓN DEL ESQUEMA DIMENSIONAL (STAR SCHEMA)
        # =========================================================
        print("🧩 Generando IDs y Tablas Dimensionales...")

        # 1. Dimensión Personal (Usuarios)
        # Tomamos nombres únicos, quitamos nulos y les asignamos un ID secuencial
        dim_personal = consolidated_df.select(
            ["nombre", "Sector_Origen"]
        ).unique().drop_nulls(subset=["nombre"]).with_row_index(name="ID_Personal", offset=1)

        # 2. Dimensión Categoría
        dim_categoria = consolidated_df.select(
            ["Category", "Sub-Category"]
        ).unique().drop_nulls(subset=["Category"]).with_row_index(name="ID_Categoria", offset=1)

        # 3. Dimensión Proyecto
        # (Aseguramos que el ID de proyecto también se incluya si existe)
        cols_proyecto = ["Proyecto", "proyecto_id"] if "proyecto_id" in consolidated_df.columns else ["Proyecto"]
        dim_proyecto = consolidated_df.select(
            cols_proyecto
        ).unique().drop_nulls(subset=["Proyecto"]).with_row_index(name="ID_Proyecto", offset=1)

        # 4. Cruzar los IDs hacia la Tabla de Hechos (Fact Table)
        # Unimos las dimensiones con la tabla maestra para traer los IDs
        fact_table = consolidated_df.join(dim_personal, on=["nombre", "Sector_Origen"], how="left")
        fact_table = fact_table.join(dim_categoria, on=["Category", "Sub-Category"], how="left")
        fact_table = fact_table.join(dim_proyecto, on=cols_proyecto, how="left")

        # 5. Generar un ID único para cada fila de registro de horas y seleccionar solo lo necesario
        fact_table = fact_table.with_row_index(name="ID_Registro", offset=1)
        
        columnas_hechos = [
            "ID_Registro", "ID_Personal", "ID_Categoria", "ID_Proyecto", 
            "Fecha", "Día", "Mes", "Cantidad de horas", "Descripción", "archivo_origen"
        ]
        
        # Filtramos para que la Fact Table solo tenga las columnas correctas
        fact_table = fact_table.select([col for col in columnas_hechos if col in fact_table.columns])
        
        # =========================================================
        # 📤 EXPORTACIÓN MULTIPLE (Usando tu función intacta)
        # =========================================================
        print("📤 Exportando Esquema Dimensional a Google Drive...")
        
        # Opcional: Mantener la tabla plana original por si alguien la quiere en Excel
        export_to_drive(gc, consolidated_df, "Productividad Equi Consolidado", CONSOLIDATED_FOLDER_ID)

        # Usamos tu función export_to_drive 4 veces seguidas para los nuevos archivos
        export_to_drive(gc, dim_personal, "Dim_Personal", CONSOLIDATED_FOLDER_ID)
        export_to_drive(gc, dim_categoria, "Dim_Categoria", CONSOLIDATED_FOLDER_ID)
        export_to_drive(gc, dim_proyecto, "Dim_Proyecto", CONSOLIDATED_FOLDER_ID)
        export_to_drive(gc, fact_table, "Fact_Timesheet", CONSOLIDATED_FOLDER_ID)

        print(f"\n✅ Pipeline completado exitosamente.")
        print(f"📊 Registros en Fact Table: {len(fact_table)}")
        
        return fact_table

    except Exception as e:
        print("\n❌ El pipeline falló. Detalle del error:")
        traceback.print_exc()
        sys.exit(1)


        print("📤 Exportando Consolidado General...")
        export_to_drive(gc, consolidated_df, "Productividad Equi Consolidado", CONSOLIDATED_FOLDER_ID)

        print(f"\n✅ Pipeline completado exitosamente.")
        print(f"\n📊 Total de registros procesados: {len(consolidated_df)} registros al archivo Productividad Equi Consolidado")
        
        return consolidated_df

    except Exception as e:
        # <-- AÑADIDO: La parte más importante. Si esto falla, rompe el script para que GitHub se entere.
        print("\n❌ El pipeline falló. Detalle del error:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()