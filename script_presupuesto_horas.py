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
    print("💸 Leyendo archivo de Costos Internos...")
    try:
        sh = gc.open_by_key(RATES_FILE_ID)
        ws = sh.worksheet(RATES_SHEET_NAME)
        raw_data = ws.get_all_values()
        
        if not raw_data or len(raw_data) < 2:
            return None
            
        headers = raw_data[0]
        data = raw_data[1:]
        df_costos = pl.DataFrame(data, schema=headers, orient="row").with_columns(pl.all().cast(pl.Utf8))
        
        # Búsqueda dinámica de las columnas (ajusta las palabras clave si es necesario)
        col_nombre = next((c for c in df_costos.columns if "NOMBRE" in c.upper()), None)
        col_costo = next((c for c in df_costos.columns if "COSTO" in c.upper()), None)
        
        if not col_nombre or not col_costo:
            print("⚠️ No se encontraron las columnas de 'Nombre' o 'Costo' en tu archivo de costos.")
            return None
            
        # Limpieza de datos (elimina signos de dólar, espacios, etc., para castear a numérico)
        df_costos = df_costos.select([
            pl.col(col_nombre).str.strip_chars().alias("nombre_costo"),
            pl.col(col_costo).str.replace_all(r"[^\d\.\,]", "").str.replace(",", ".").cast(pl.Float64, strict=False).alias("Costo_interno")
        ])
        return df_costos
    except Exception as e:
        print(f"🚨 Error obteniendo archivo de costos: {e}")
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
            print("🧮 Calculando costos por proyecto...")
            # Normalizamos los nombres a minúsculas para asegurar que hagan match ("Juan Perez" == "juan perez")
            master_presupuesto = master_presupuesto.with_columns(pl.col("nombre").str.strip_chars().str.to_lowercase().alias("nombre_join"))
            df_costos = df_costos.with_columns(pl.col("nombre_costo").str.strip_chars().str.to_lowercase().alias("nombre_join"))
            
            # Left Join
            master_presupuesto = master_presupuesto.join(df_costos, on="nombre_join", how="left")
            
            # 🆕 APLICAR LA FÓRMULA DE COSTO
            master_presupuesto = master_presupuesto.with_columns(
                ((pl.col("Horas_Presupuestadas") / 8) * pl.col("Costo_interno")).alias("Costo_Total_Proyecto")
            ).drop("nombre_join") # Limpiamos la columna temporal
        else:
            # En caso de fallar, creamos columnas vacías para no romper el select final
            master_presupuesto = master_presupuesto.with_columns([
                pl.lit(None).alias("Costo_interno"),
                pl.lit(None).alias("Costo_Total_Proyecto")
            ])

        # Selección final de columnas incluyendo las nuevas métricas
        master_presupuesto = master_presupuesto.select([
            "ID_Presupuesto", "proyecto_id", "Proyecto", "nombre", 
            "Horas_Presupuestadas", "Costo_interno", "Costo_Total_Proyecto", "archivo_origen"
        ])
        
        print(f"\n📤 Exportando a Carpeta DWH: {DWH_FOLDER_ID}")
        export_to_drive(gc, master_presupuesto, "Fact_Presupuesto_Horas", DWH_FOLDER_ID)
        
        print(f"\n✅ Pipeline Finalizado. Revisa tu archivo 'Fact_Presupuesto_Horas' en Drive.")
    else:
        print("❌ No se encontraron datos válidos en estos dos archivos de prueba.")

if __name__ == "__main__":
    run_presupuestos_pipeline()