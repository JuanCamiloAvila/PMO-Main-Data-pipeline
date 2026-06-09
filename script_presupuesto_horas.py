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
import functools

# ==============================================================================
# 1. CONFIGURACIÓN
# ==============================================================================
TEST_FILE_IDS = [
    "1T8cwmU7fWuTQsoafPfxMqZ0S_6HUBJXMC-dRyzXk5GM",
    "1C9B3hrNI9heC7xf37bCC2KifDGzhhSC7at46kyg6y6k"
]

SOURCES_CONFIG = {
    "2026": {
        "folders": [
            #{"id": "1EbqyjMb841artvPenWBuu2PcJuOc4w_Y", "label": "Estratégico"},
            #{"id": "15p3qq7KtFDgDqZKh1vFCkKrczDkNIY7k", "label": "Empresarial"},
            #{"id": "1hA9Sb6nYY9vuaxNnrTG7tYginYhDVX-X", "label": "Desarrollo"},
            #{"id": "1dLYU7aYYkPZKN012vtmvj15a01gWsu1_", "label": "Directorio"},
            {"id": "1uFZXGHpfab4iL-mvmzH0i5boNphhZHEj", "label": "Testeo - Nuevla Plantilla Flujo de Presupuesto"},
        ]
    },
    "2025": {
        "folders": [
            #{"id": "1m-LdkuaKc4j-EVfMCLF6gpr_IUnFzaUF", "label": "Solicitudes proyectos pasados"},
            #{"id": "1Z-cWyN3qjCMRAWG1SCsJh2MD7s-7nk4mS", "label": "Proyectos Estratégicos"},
            #{"id": "1M9xSJl-7riGk-IUfg7bH1YirMtRkdFvB", "label": "Empresarial"},
            #{"id": "1nuxC_fhk6arz4N1mVCPUWe-3bkQW3_Yp", "label": "Desarrollo"},
            #{"id": "1nVhpyaW0d3uFQdF7t6_qgmTJpkBmL8xr", "label": "Directorio"}
        ]
    }
}
 
DWH_FOLDER_ID = "1_8cyY32pxRXU3Au0OZOor1wNN7uXO-wr"
RATES_FILE_ID = "1PFVuVLKbNWh2TJEG-x2K8-KvHQBuqbRWurAx1PI67FA" 
RATES_SHEET_NAME = "Rates"

# ==============================================================================
# 2. AUTENTICACIÓN Y EXPORTACIÓN
# ==============================================================================
def api_retry(max_retries=6):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    if any(code in str(e) for code in ['429', '500', '502', '503']):
                        wait_time = (attempt + 1) * 20
                        print(f"    ⚠️ Límite de cuota o red. Reintentando en {wait_time}s... ({attempt+1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        raise e
            raise Exception(f"❌ Falló tras {max_retries} reintentos en: {func.__name__}")
        return wrapper
    return decorator

@api_retry()
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    token_str = os.environ.get('GOOGLE_OAUTH_TOKEN')
    
    if token_str:
        print("🔑 Modo Nube: Usando token de entorno...")
        creds = Credentials.from_authorized_user_info(json.loads(token_str), scopes)
    else:
        print("🔑 Modo Local: Usando archivo token.json...")
        creds = Credentials.from_authorized_user_file('token.json', scopes)
        
    return gspread.authorize(creds)
        
@api_retry()
def export_to_drive(gc, df: pl.DataFrame, file_name: str, folder_id: str):
    if df is None or df.is_empty(): return
    datos_exportar = [list(df.columns)]
    for row in df.rows():
        datos_exportar.append(["" if val is None else val for val in row])
    
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
    print(f"      ✅ Guardado en DWH: {file_name}")

# ==============================================================================
# 3. EXTRACCIÓN Y RECORTADO DE TABLAS
# ==============================================================================

def limpiar_nombre(nombre):
    if not nombre: return ""
    import unicodedata
    nombre = str(nombre).lower().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', nombre)
                  if unicodedata.category(c) != 'Mn')

def extraer_equipo_interno(raw_rows, file_name, formato="LEGACY"):
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
    col_horas = next((c for c in df.columns if "CANTIDAD DE HORAS" in c.upper() or "HORAS PRESUPUESTADAS" in c.upper() or "DÍAS PRESUPUESTADOS" in c.upper() or "DIAS PRESUPUESTADOS" in c.upper()), None)
    col_rate = next((c for c in df.columns if "RATE" in c.upper()), None)
    col_rol = next((c for c in df.columns if "ROL DE PROYECTO" in c.upper() or "CARGO EN" in c.upper() or "ROL" == c.upper().strip() or "CARGO" == c.upper().strip()), None)
    
    if not col_nombre or not col_horas or not col_rate: return None

    df = df.filter(
        (pl.col(col_nombre).str.strip_chars() != "") & 
        (~pl.col(col_nombre).str.to_uppercase().str.contains("INSERTAR"))
    )
    
    es_dias = "DÍA" in col_horas.upper() or "DIA" in col_horas.upper()
    factor = 8.0 if es_dias else 1.0

    columnas_nuevas = [
        pl.col(col_nombre).alias("nombre"),
        (pl.col(col_horas).str.replace(",", ".").cast(pl.Float64, strict=False) * factor).alias("Horas_Presupuestadas"),
        pl.col(col_rate).str.replace_all(r"[^\d\.\,]", "").str.replace(",", ".")
            .cast(pl.Float64, strict=False).fill_null(0.0).alias("Costo_interno")
    ]
    
    if col_rol:
        columnas_nuevas.append(pl.col(col_rol).alias("Rol"))
    else:
        columnas_nuevas.append(pl.lit("").alias("Rol"))
        
    df = df.with_columns(columnas_nuevas)
    
    df = df.filter(pl.col("Horas_Presupuestadas").is_not_null())
    
    # Limpiar nombre según el formato
    if formato == "NUEVO":
        nombre_temp = file_name
        for prefijo in ["NUEVO", "nuevo", "Nuevo", "MONITOREO", "monitoreo", "Monitoreo"]:
            nombre_temp = nombre_temp.replace(prefijo, "").strip()
        indice_inicio = 0
        for i, char in enumerate(nombre_temp):
            if char.isdigit():
                indice_inicio = i
                break
        nombre_archivo_limpio = nombre_temp[indice_inicio:].split(".")[0].strip()
    else:
        nombre_archivo_limpio = file_name.replace("Productividad: ", "").strip()
    
    df = df.with_columns([
        pl.lit(file_name).alias("archivo_origen"),
        pl.lit(nombre_archivo_limpio).alias("Proyecto")
    ])

    return df.select(["archivo_origen", "Proyecto", "nombre", "Rol", "Horas_Presupuestadas", "Costo_interno"])

@api_retry()
def obtener_directorio_correos(gc):
    print("📇 Leyendo Master de Rates solo como Directorio (Nombres y Correos)...")
    try:
        sh = gc.open_by_key(RATES_FILE_ID)
        ws = sh.worksheet(RATES_SHEET_NAME)
        raw_data = ws.get_all_values()
        
        if not raw_data or len(raw_data) < 2: return None
        
        headers = raw_data[0]
        df_raw = pl.DataFrame(raw_data[1:], schema=headers, orient="row")
        
        columnas_busqueda = ["llave_cruce", "nombre_oficial", "correo_maestro"]

        df_principal = df_raw.select([
            pl.col("Nombre").map_elements(limpiar_nombre, return_dtype=pl.Utf8).alias("llave_cruce"),
            pl.col("Nombre_oficial").alias("nombre_oficial"),
            pl.col("Correo").alias("correo_maestro")
        ]).filter(pl.col("llave_cruce") != "").select(columnas_busqueda)

        df_alias = df_raw.select([
            pl.col("Alias").str.split(","), 
            pl.col("Nombre_oficial").alias("nombre_oficial"),
            pl.col("Correo").alias("correo_maestro")
        ]).explode("Alias").with_columns(
            pl.col("Alias").map_elements(limpiar_nombre, return_dtype=pl.Utf8).alias("llave_cruce")
        ).filter(
            (pl.col("llave_cruce") != "") & 
            (pl.col("llave_cruce").str.len_chars() > 2) 
        ).select(columnas_busqueda)

        resultado = pl.concat([df_principal, df_alias])
        return resultado.unique(subset=["llave_cruce"], keep="first")
        
    except Exception as e:
        print(f"🚨 Error en Master de Rates: {e}")
        return None

# ==============================================================================
# 4. PIPELINE PRINCIPAL 
# ==============================================================================

@api_retry()
def abrir_archivo_protegido(gc, file_id):
    return gc.open_by_key(file_id)

@api_retry()
def listar_archivos_protegido(gc, folder_id):
    return gc.list_spreadsheet_files(folder_id=folder_id)

@api_retry(max_retries=6)
def leer_valores_pestana(sh, nombre_pestana):
    try:
        ws = sh.worksheet(nombre_pestana)
        return ws.get('A1:Z300') 
    except gspread.exceptions.WorksheetNotFound:
        return None

def run_presupuestos_pipeline():
    print("🚀 Iniciando Extracción de Horas Presupuestadas...")
    gc = get_gspread_client()
    
    archivos_a_procesar = []
    archivos_fallidos = []

    print("📁 Escaneando carpetas de Google Drive...")
    for anio, config in SOURCES_CONFIG.items():
        for carpeta in config["folders"]:
            try:
                files_in_folder = listar_archivos_protegido(gc, carpeta["id"])
                
                archivos_en_esta_carpeta = []
                for f in files_in_folder:
                    nombre = f.get('name', '')
                    es_legacy = nombre.startswith("Productividad:") and "Template" not in nombre and "-" in nombre
                    es_nuevo = nombre.upper().startswith("MONITOREO") and "COPIA" not in nombre.upper()
                    
                    if es_legacy or es_nuevo:
                        archivos_en_esta_carpeta.append({
                            "id": f['id'],
                            "anio": anio,
                            "formato": "NUEVO" if es_nuevo else "LEGACY"
                        })
                
                archivos_a_procesar.extend(archivos_en_esta_carpeta)
                print(f"   ✅ {anio} - {carpeta['label']}: {len(archivos_en_esta_carpeta)} archivos válidos encontrados.")
            except Exception as e:
                print(f"   🚨 Error accediendo a carpeta {carpeta['label']} ({anio}): {e}")

    archivos_unicos = {obj["id"]: obj for obj in archivos_a_procesar}
    archivos_a_procesar = list(archivos_unicos.values())
    
    total_archivos = len(archivos_a_procesar)
    if total_archivos == 0:
        print("❌ No se encontraron archivos válidos en las carpetas configuradas.")
        return

    procesados = 0
    contador_lock = threading.Lock()
    lista_dfs = []

    def worker(file_info):
        nonlocal procesados
        time.sleep(2) 
        
        file_id = file_info["id"]
        anio_archivo = file_info["anio"]
        formato = file_info.get("formato", "LEGACY")
        
        res = None
        file_name = f"ID Desconocido: {file_id}" 
        
        try:
            sh = abrir_archivo_protegido(gc, file_id)
            if sh:
                file_name = sh.title
                
                # Para el nuevo formato y el viejo la pestaña se asume "Equipo" por ahora. 
                # Si en el nuevo formato la pestaña se llama distinto, lo ajustaremos.
                nombre_pestana = "Equipo" 
                raw_data = leer_valores_pestana(sh, nombre_pestana)
                
                if raw_data is not None:
                    res = extraer_equipo_interno(raw_data, file_name, formato)
                    
                with contador_lock:
                    procesados += 1
                    if raw_data is None:
                        print(f"[{procesados}/{total_archivos}] [📅 {anio_archivo}] [{formato}] ⏭️ Ignorado (Sin pestaña '{nombre_pestana}'): {file_name}")
                    elif res is not None:
                        filas_extraidas = len(res)
                        if filas_extraidas == 0:
                            print(f"[{procesados}/{total_archivos}] [📅 {anio_archivo}] ⚠️ Extraído pero con 0 FILAS VÁLIDAS: {file_name}")
                        else:
                            print(f"[{procesados}/{total_archivos}] [📅 {anio_archivo}] ✅ Extraído ({filas_extraidas} filas): {file_name}")
                    else:
                        print(f"[{procesados}/{total_archivos}] [📅 {anio_archivo}] ⚠️ Extraído pero sin datos válidos (Falta columna Rate o Horas): {file_name}")
                        
        except Exception as e:
            with contador_lock:
                procesados += 1
                print(f"[{procesados}/{total_archivos}] [📅 {anio_archivo}] ❌ Error definitivo: {file_name}")
                archivos_fallidos.append({
                    "anio": anio_archivo,
                    "nombre": file_name,
                    "id": file_id
                })
        return res

    with ThreadPoolExecutor(max_workers=2) as executor:
        resultados = list(executor.map(worker, archivos_a_procesar))

    for r in resultados:
        if r is not None and not r.is_empty(): 
            lista_dfs.append(r)

    if archivos_fallidos:
        print("\n" + "="*70)
        print("🚨 REPORTE DE ARCHIVOS NO ESCANEADOS (FALLARON TRAS TODOS LOS REINTENTOS)")
        print("="*70)
        for f in archivos_fallidos:
            print(f"📅 {f['anio']} | 📄 {f['nombre']} | 🔗 ID: {f['id']}")
        print("="*70 + "\n")

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
        
        df_directorio = obtener_directorio_correos(gc)
        
        if df_directorio is not None:
            master_presupuesto = master_presupuesto.with_columns(
                pl.col("nombre").map_elements(limpiar_nombre, return_dtype=pl.Utf8).alias("llave_cruce")
            )
            
            master_presupuesto = master_presupuesto.join(df_directorio, on="llave_cruce", how="left")
            
            master_presupuesto = master_presupuesto.with_columns([
                # ✨ CORRECCIÓN AQUÍ: Si el Master no lo tiene o devuelve un campo vacío, conserva el original
                pl.when(pl.col("nombre_oficial").is_null() | (pl.col("nombre_oficial") == ""))
                .then(pl.col("nombre"))
                .otherwise(pl.col("nombre_oficial"))
                .alias("nombre"),
                
                ((pl.col("Horas_Presupuestadas") / 8) * pl.col("Costo_interno").fill_null(0)).alias("Costo_Total_Proyecto"),
                pl.col("correo_maestro").alias("correo_electronico")
            ])
        else:
            master_presupuesto = master_presupuesto.with_columns([
                ((pl.col("Horas_Presupuestadas") / 8) * pl.col("Costo_interno").fill_null(0)).alias("Costo_Total_Proyecto"),
                pl.lit(None).alias("correo_electronico")
            ])
        
        master_presupuesto = master_presupuesto.select([
            "ID_Presupuesto", 
            "proyecto_id", 
            "Proyecto", 
            "nombre", 
            "Rol",
            "Horas_Presupuestadas", 
            "Costo_interno", 
            "Costo_Total_Proyecto", 
            "archivo_origen",
            "correo_electronico" 
        ])
        
        print(f"\n📤 Exportando a Carpeta DWH: {DWH_FOLDER_ID}")
        export_to_drive(gc, master_presupuesto, "Fact_Presupuesto_Horas", DWH_FOLDER_ID)
        
        print(f"\n✅ Pipeline Finalizado. Revisa tu archivo 'Fact_Presupuesto_Horas' en Drive.")
    else:
        print("❌ No se encontraron datos válidos.")

if __name__ == "__main__":
    run_presupuestos_pipeline()