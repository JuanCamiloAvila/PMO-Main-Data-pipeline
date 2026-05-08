import os
import sys
import json
import polars as pl
import gspread
from google.oauth2.credentials import Credentials
import traceback
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import unicodedata

# ==============================================================================
# 1. CONFIGURACIÓN
# ==============================================================================
TEST_FILE_IDS = [
    "1T8cwmU7fWuTQsoafPfxMqZ0S_6HUBJXMC-dRyzXk5GM",
    "1C9B3hrNI9heC7xf37bCC2KifDGzhhSC7at46kyg6y6k"
]

DWH_FOLDER_ID = "1_8cyY32pxRXU3Au0OZOor1wNN7uXO-wr"

# 🆕 VARIABLES PARA EL ARCHIVO DE COSTOS (⚠️ REEMPLAZA ESTOS VALORES)
RATES_FILE_ID = "1PFVuVLKbNWh2TJEG-x2K8-KvHQBuqbRWurAx1PI67FA" 
RATES_SHEET_NAME = "Rates" # Nombre exacto de la pestaña donde están los costos

# ==============================================================================
# 2. AUTENTICACIÓN Y EXPORTACIÓN
# ==============================================================================
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    token_str = os.environ.get('GOOGLE_OAUTH_TOKEN')
    if token_str:
        return gspread.authorize(Credentials.from_authorized_user_info(json.loads(token_str), scopes))
    return gspread.authorize(Credentials.from_authorized_user_file('token.json', scopes))

def export_to_drive(gc, df: pl.DataFrame, file_name: str, folder_id: str):
    if df is None or df.is_empty(): return
    datos_exportar = [list(df.columns)]
    for row in df.rows():
        datos_exportar.append(["" if val is None else val for val in row])
    
    files = gc.list_spreadsheet_files(folder_id=folder_id)
    file_id = next((f['id'] for f in files if f['name'] == file_name), None)
    
    intentos, exito = 0, False
    while intentos < 3 and not exito:
        try:
            if file_id: sh = gc.open_by_key(file_id)
            else: sh = gc.create(file_name, folder_id=folder_id)
                
            try: ws = sh.worksheet("Datos")
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.sheet1
                ws.update_title("Datos")
                
            ws.clear()
            ws.update(datos_exportar, value_input_option="USER_ENTERED")
            exito = True
            print(f"      ✅ Guardado en DWH: {file_name}")
            time.sleep(2)
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                intentos += 1
                time.sleep(20 * intentos)
            else: raise e

# ==============================================================================
# 3. EXTRACCIÓN Y RECORTADO DE TABLAS
# ==============================================================================

def limpiar_nombre(nombre):
    if not nombre: return ""
    import unicodedata
    nombre = str(nombre).lower().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', nombre)
                  if unicodedata.category(c) != 'Mn')

def extraer_equipo_interno(raw_rows, file_name):
    if not raw_rows or len(raw_rows) < 2: return None
    
    cleaned_rows = []
    for row in raw_rows:
        clean_row = ["" if str(cell).strip().startswith("#") else cell for cell in row]
        cleaned_rows.append(clean_row)
        
    raw_rows = cleaned_rows
    
    header_idx = -1
    for i, row in enumerate(raw_rows[:30]): 
        row_upper = [str(cell).upper().strip() for cell in row]
        if any("NOMBRE COMPLETO" in cell for cell in row_upper):
            header_idx = i
            break
            
    if header_idx == -1: return None
    raw_headers = raw_rows[header_idx]
    
    data_rows = []
    for row in raw_rows[header_idx + 1:]:
        row_upper = [str(cell).upper().strip() for cell in row]
        if any("TOTAL" in cell or "EQUIPO EXTERNO" in cell for cell in row_upper):
            break
        data_rows.append(row)
    
    if not data_rows: return None

    max_cols = max(len(raw_headers), max((len(r) for r in data_rows), default=0))
    padded_headers = raw_headers + [""] * (max_cols - len(raw_headers))
    
    headers, vistos = [], set()
    for i, h in enumerate(padded_headers):
        nombre_base = str(h).strip() if str(h).strip() else f"column_{i}"
        nombre_final = nombre_base
        contador = 1
        while nombre_final in vistos:
            nombre_final = f"{nombre_base}_{contador}"
            contador += 1
        vistos.add(nombre_final)
        headers.append(nombre_final)

    normalized_rows = [row + [""] * (max_cols - len(row)) for row in data_rows]
    df = pl.DataFrame(normalized_rows, schema=headers, orient="row").with_columns(pl.all().cast(pl.Utf8))
    
    col_nombre = next((c for c in df.columns if "NOMBRE COMPLETO" in c.upper()), None)
    col_horas = next((c for c in df.columns if "CANTIDAD DE HORAS" in c.upper() or "HORAS PRESUPUESTADAS" in c.upper()), None)
    
    if not col_nombre or not col_horas: return None

    df = df.filter(
        (pl.col(col_nombre).str.strip_chars() != "") & 
        (~pl.col(col_nombre).str.to_uppercase().str.contains("INSERTAR"))
    )
    
    df = df.with_columns([
        pl.col(col_nombre).alias("nombre"),
        pl.col(col_horas).str.replace(",", ".").cast(pl.Float64, strict=False).alias("Horas_Presupuestadas")
    ])
    
    df = df.filter(pl.col("Horas_Presupuestadas").is_not_null())
    nombre_archivo_limpio = file_name.replace("Productividad: ", "").strip()
    
    df = df.with_columns([
        pl.lit(file_name).alias("archivo_origen"),
        pl.lit(nombre_archivo_limpio).alias("Proyecto")
    ])

    return df.select(["archivo_origen", "Proyecto", "nombre", "Horas_Presupuestadas"])

# 🆕 NUEVA FUNCIÓN: Extraer Costos
def obtener_costos_internos(gc):
    print("💸 Leyendo Master de Rates y mapeando Alias...")
    try:
        sh = gc.open_by_key(RATES_FILE_ID)
        ws = sh.worksheet(RATES_SHEET_NAME)
        raw_data = ws.get_all_values()
        
        if not raw_data or len(raw_data) < 2: return None
        
        headers = raw_data[0]
        df_raw = pl.DataFrame(raw_data[1:], schema=headers, orient="row")
        
        columnas_busqueda = ["llave_cruce", "nombre_oficial", "correo_maestro", "tasa"]

        # 1. Nombres Principales (Prioridad 1)
        df_principal = df_raw.select([
            pl.col("Nombre").map_elements(limpiar_nombre, return_dtype=pl.Utf8).alias("llave_cruce"),
            pl.col("Nombre_oficial").alias("nombre_oficial"),
            pl.col("Correo").alias("correo_maestro"),
            pl.col("Costo interno").str.replace_all(r"[^\d\.\,]", "").str.replace(",", ".")
                .cast(pl.Float64, strict=False).fill_null(0.0).alias("tasa")
        ]).filter(pl.col("llave_cruce") != "").select(columnas_busqueda)

        # 2. Alias (Prioridad 2 - Solo si el alias tiene más de 2 letras)
        df_alias = df_raw.select([
            pl.col("Alias").str.split(","), 
            pl.col("Nombre_oficial").alias("nombre_oficial"),
            pl.col("Correo").alias("correo_maestro"),
            pl.col("Costo interno").str.replace_all(r"[^\d\.\,]", "").str.replace(",", ".")
                .cast(pl.Float64, strict=False).fill_null(0.0).alias("tasa")
        ]).explode("Alias")
        
        df_alias = df_alias.with_columns(
            pl.col("Alias").map_elements(limpiar_nombre, return_dtype=pl.Utf8).alias("llave_cruce")
        ).filter(
            (pl.col("llave_cruce") != "") & 
            (pl.col("llave_cruce").str.len_chars() > 2) # 🛡️ Evita que alias como "Ma" crucen con "Mario"
        ).select(columnas_busqueda)

        # 3. Consolidamos con prioridad: El nombre principal va PRIMERO
        resultado = pl.concat([df_principal, df_alias])
        
        # DEBUG: Imprimir si hay colisiones (opcional pero recomendado)
        duplicados = resultado.filter(pl.col("llave_cruce").is_duplicated())
        if not duplicados.is_empty():
            print("⚠️ Hay nombres o alias idénticos en el Master para personas distintas:")
            print(duplicados.select(["llave_cruce", "nombre_oficial"]))

        # Mantenemos el PRIMERO que encuentre (por eso df_principal va arriba en el concat)
        return resultado.unique(subset=["llave_cruce"], keep="first")
        
    except Exception as e:
        print(f"🚨 Error en Master de Rates: {e}")
        return None

# ==============================================================================
# 4. PIPELINE PRINCIPAL 
# ==============================================================================
def run_presupuestos_pipeline():
    print("🚀 Iniciando Extracción de Horas Presupuestadas...")
    gc = get_gspread_client()
    
    total_archivos = len(TEST_FILE_IDS)
    procesados = 0
    contador_lock = threading.Lock()
    lista_dfs = []

    def worker(file_id):
        nonlocal procesados
        res = None
        try:
            sh = gc.open_by_key(file_id)
            file_name = sh.title
            try:
                ws = sh.worksheet("Equipo")
                raw_data = ws.get_all_values()
                res = extraer_equipo_interno(raw_data, file_name)
            except gspread.exceptions.WorksheetNotFound:
                pass 
                
            with contador_lock:
                procesados += 1
                if res is not None:
                    print(f"[{procesados}/{total_archivos}] ✅ Extraído: {file_name}")
                else:
                    print(f"[{procesados}/{total_archivos}] ⏭️ Ignorado (Sin datos válidos): {file_name}")
        except Exception as e:
            print(f"🚨 Error abriendo archivo ID {file_id}: {e}")
        return res

    with ThreadPoolExecutor(max_workers=3) as executor:
        resultados = list(executor.map(worker, TEST_FILE_IDS))

    for r in resultados:
        if r is not None: lista_dfs.append(r)

    if lista_dfs:
        print("\n⚡ Consolidando tabla de Horas Presupuestadas...")
        master_presupuesto = pl.concat(lista_dfs, how="diagonal")
        
        master_presupuesto = master_presupuesto.with_columns(
            pl.col("Proyecto").str.extract(r"^([\d-]+)", 1).str.replace_all("-", "_").alias("proyecto_id")
        )
        
        master_presupuesto = master_presupuesto.with_columns(
            pl.when(pl.col("proyecto_id").is_not_null() & pl.col("proyecto_id").str.contains(r"^\d+$"))
            .then(pl.lit("'") + pl.col("proyecto_id"))
            .otherwise(pl.col("proyecto_id"))
            .alias("proyecto_id")
        )
        
        master_presupuesto = master_presupuesto.with_row_index(name="ID_Presupuesto", offset=1)
        
        # 🆕 OBTENER COSTOS Y HACER EL CRUCE (JOIN)
        df_costos = obtener_costos_internos(gc)
        
        if df_costos is not None:
            # Preparamos el nombre del presupuesto para cruzar
            master_presupuesto = master_presupuesto.with_columns(
                pl.col("nombre").map_elements(limpiar_nombre, return_dtype=pl.Utf8).alias("llave_cruce")
            )
            
            # Join
            master_presupuesto = master_presupuesto.join(df_costos, on="llave_cruce", how="left")
            
            # Asignamos valores finales
            master_presupuesto = master_presupuesto.with_columns([
                pl.coalesce([pl.col("nombre_oficial"), pl.col("nombre")]).alias("nombre"),
                pl.col("tasa").alias("Costo_interno"),
                ((pl.col("Horas_Presupuestadas") / 8) * pl.col("tasa").fill_null(0)).alias("Costo_Total_Proyecto"),
                pl.col("correo_maestro").alias("correo_electronico")
            ])
        
        # --- SELECCIÓN FINAL (Orden que pediste para Looker) ---
        master_presupuesto = master_presupuesto.select([
            "ID_Presupuesto", 
            "proyecto_id", 
            "Proyecto", 
            "nombre", 
            "Horas_Presupuestadas", 
            "Costo_interno", 
            "Costo_Total_Proyecto", 
            "archivo_origen",
            "correo_electronico" # ✨ Siempre al final
        ])
        
        print(f"\n📤 Exportando a Carpeta DWH: {DWH_FOLDER_ID}")
        export_to_drive(gc, master_presupuesto, "Fact_Presupuesto_Horas", DWH_FOLDER_ID)
        
        print(f"\n✅ Pipeline Finalizado. Revisa tu archivo 'Fact_Presupuesto_Horas' en Drive.")
    else:
        print("❌ No se encontraron datos válidos en estos dos archivos de prueba.")

if __name__ == "__main__":
    run_presupuestos_pipeline()