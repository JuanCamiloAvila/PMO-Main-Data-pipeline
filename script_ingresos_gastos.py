import os
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
FOLDER_IDS_PRESUPUESTOS = ["18Ma7mj63Egs_KfyLtkdOPQnvBs6__aGw"]
MASTER_SPREADSHEET_ID = "1vUcnKrp5EfCbW5mh3L76x_UoyB4m9BPhJ_pKHPbxsGM"

# 1. ORDEN_MAESTRO: Agrega "Fecha" cerca del inicio
ORDEN_MAESTRO = [
    "archivo_origen", "Tipo_Movimiento", "Fecha", "Proyecto", "País de facturación", 
    "Categoría", "Tipo de gasto", "Descripción", "Producto/Entregable/Servicio", 
    "Monto sin Impuestos", "IGV/IVA/Otros", "Monto con Impuestos", 
    "Moneda", "TC", "USD", "Fecha de entrega del producto", 
    "Fecha de emisión del comprobante", "Situación", "Fecha de factura proveedor"
]

TRADUCTOR_INGRESOS = {
    "Proyecto / Cuenta analítica" : "Proyecto",
    "2" : "Proyecto"
}

# 2. COLUMNAS_INGRESOS: Agrega "Fecha" al principio
COLUMNAS_INGRESOS = [
    "Fecha", "Proyecto", "País de facturación", "Producto/Entregable/Servicio", 
    "Monto sin Impuestos", "IGV/IVA/Otros", "Monto con Impuestos", 
    "Moneda", "TC", "USD", "Fecha de entrega del producto", 
    "Fecha de emisión del comprobante", "Situación"
]

TRADUCTOR_GASTOS = {
    "Monto Total / (Monto sin Impuestos)": "Monto sin Impuestos",
    "SItuación": "Situación"
}

# 3. COLUMNAS_GASTOS: Agrega "Fecha" al principio
COLUMNAS_GASTOS = [
    "Fecha", "Proyecto", "País de facturación", "Categoría", "Tipo de gasto", 
    "Descripción", "Fecha de factura proveedor", "Monto sin Impuestos", 
    "IGV/IVA/Otros", "Monto con Impuestos", "Moneda", "TC", "USD", "Situación"
]

# ==============================================================================
# 2. FUNCIONES NÚCLEO
# ==============================================================================
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    token_str = os.environ.get('GOOGLE_OAUTH_TOKEN')
    if token_str:
        return gspread.authorize(Credentials.from_authorized_user_info(json.loads(token_str), scopes))
    return gspread.authorize(Credentials.from_authorized_user_file('token.json', scopes))

def export_to_drive(gc, df: pl.DataFrame, file_id: str, tab_name: str):
    if df is None or df.is_empty(): 
        return
    
    datos_exportar = [list(df.columns)]
    for row in df.rows():
        datos_exportar.append(["" if val is None else val for val in row])
    
    intentos, exito = 0, False
    while intentos < 3 and not exito:
        try:
            sh = gc.open_by_key(file_id) 
            try:
                ws = sh.worksheet(tab_name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=tab_name, rows="1000", cols="26")
            ws.clear() 
            ws.update(datos_exportar, value_input_option="USER_ENTERED")
            exito = True
            print(f"📤 Exportación exitosa a {tab_name} ({len(df)} filas)")
            time.sleep(2)
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                intentos += 1
                time.sleep(20 * intentos)
            else: raise e

def limpiar_dataframe_pmo(raw_rows, file_name, tipo, traductor):
    if not raw_rows or len(raw_rows) < 2: return None
    
    # --- 🎯 1. RADAR DE ENCABEZADOS ---
    header_idx = 0
    for i, row in enumerate(raw_rows[:15]):
        row_upper = [str(cell).upper() for cell in row]
        if any("PROYECTO" in cell or "MONTO" in cell or "SITUACI" in cell or "FACTURA" in cell for cell in row_upper):
            header_idx = i
            break

    raw_headers = raw_rows[header_idx]
    data_rows = raw_rows[header_idx + 1:]
    
    if not data_rows: return None

    # --- 2. NORMALIZACIÓN ---
    max_cols = max(len(raw_headers), max((len(r) for r in data_rows), default=0))
    padded_headers = raw_headers + [""] * (max_cols - len(raw_headers))
    headers = []
    vistos = set()
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
    
    try:
        df = pl.DataFrame(normalized_rows, schema=headers, orient="row").with_columns(pl.all().cast(pl.Utf8))
        
        # --- 2️⃣ MAPEO INTELIGENTE Y SEGURO (Manejo de duplicados) ---
        mapeo_seguro = {}
        nombres_ya_usados = set()
        
        for col_real in df.columns:
            col_upper = col_real.upper()
            
            objetivo = traductor.get(col_real) if traductor else None
            
            if not objetivo:
                if col_upper.startswith("PROYECTO"): 
                    objetivo = "Proyecto"
                elif col_real == "2" or col_upper.startswith("MONTO SIN"): 
                    objetivo = "Monto sin Impuestos"
                elif col_upper.startswith("MONTO CON"): 
                    objetivo = "Monto con Impuestos"
                elif col_upper.startswith("SITUACI"): 
                    objetivo = "Situación"
                else:
                    objetivo = col_real
            
            if objetivo in nombres_ya_usados:
                objetivo = col_real 
                contador = 1
                while objetivo in nombres_ya_usados:
                    objetivo = f"{col_real}_dup{contador}"
                    contador += 1
            
            nombres_ya_usados.add(objetivo)
            
            if objetivo != col_real:
                mapeo_seguro[col_real] = objetivo
        
        df = df.rename(mapeo_seguro)
        
        # --- 🎯 CREACIÓN DE LA COLUMNA FECHA UNIFICADA ---
        if tipo == "Ingreso" and "Fecha de emisión del comprobante" in df.columns:
            df = df.with_columns(pl.col("Fecha de emisión del comprobante").alias("Fecha"))
        elif tipo == "Gasto" and "Fecha de factura proveedor" in df.columns:
            df = df.with_columns(pl.col("Fecha de factura proveedor").alias("Fecha"))
        else:
            df = df.with_columns(pl.lit("").alias("Fecha"))

        # --- 🧹 4. PURGA DE FILAS FANTASMA ---
        columnas_clave = [c for c in ["Proyecto", "Situación", "Monto con Impuestos", "Descripción", "Fecha"] if c in df.columns]
        if columnas_clave:
            condicion_datos_reales = pl.lit(False)
            for c in columnas_clave:
                condicion_datos_reales = condicion_datos_reales | (pl.col(c).cast(pl.Utf8).str.strip_chars() != "")
            df = df.filter(condicion_datos_reales)

        total_inicial = len(df) 
        
        # --- 🛡️ 5. FILTROS ESTRICTOS FINANCIEROS ---
        if "Situación" in df.columns:
            df = df.filter(pl.col("Situación").cast(pl.Utf8).str.to_uppercase().str.strip_chars().is_in(["REAL", "PROYECTADO"]))

        if "Monto con Impuestos" in df.columns:
            df = df.with_columns(
                pl.col("Monto con Impuestos").cast(pl.Utf8)
                .str.replace_all(r"[^0-9,.]", "")
                .str.replace_all(r"\.", "") 
                .str.replace(",", ".")
                .alias("_temp_monto")
            )
            df = df.filter((pl.col("_temp_monto") != "") & (pl.col("_temp_monto").cast(pl.Float64, strict=False) > 0)).drop("_temp_monto")

        if tipo == "Gasto":
            df = df.filter(
                (pl.col("Fecha").cast(pl.Utf8).str.strip_chars() != "") &
                (pl.col("Fecha").is_not_null())
            )

        # --- 📣 6. AUDITORÍA SIMPLE EN CONSOLA ---
        filas_finales = len(df)
        if total_inicial > filas_finales:
            print(f"   🔎 [{tipo}] {file_name}: {total_inicial} reales extraídas -> {filas_finales} válidas.")

        # --- 7. SELECCIÓN FINAL ---
        cols_finales = COLUMNAS_INGRESOS if tipo == "Ingreso" else COLUMNAS_GASTOS
        presentes = [c for c in cols_finales if c in df.columns]
        
        if df.is_empty() or not presentes: return None

        return df.select(presentes).with_columns([pl.lit(tipo).alias("Tipo_Movimiento"), pl.lit(file_name).alias("archivo_origen")])
    except Exception as e: 
        print(f"❌ Error estructurando {file_name} ({tipo}): {e}")
        return None

# ==============================================================================
# 3. EJECUCIÓN
# ==============================================================================
def run_finanzas_pipeline():
    print("🚀 Iniciando Híbrido Maestro (Multithreading + Batching + Auditoría)...")
    gc = get_gspread_client()
    
    files = []
    for f_url in FOLDER_IDS_PRESUPUESTOS:
        f_id = f_url.split('/folders/')[-1].split('?')[0].strip()
        files.extend(gc.list_spreadsheet_files(folder_id=f_id))
    
    files_validos = [f for f in {fi['id']: fi for fi in files}.values() 
                     if f['name'].upper().startswith("NUEVO") and "COPIA" not in f['name'].upper()]

    total_archivos = len(files_validos)
    procesados = 0
    contador_lock = threading.Lock()
    
    # 3️⃣ SISTEMA DE RASTREO
    archivos_exitosos = []

    def worker(f):
        nonlocal procesados
        res = None
        intentos = 0
        while intentos < 3:
            try:
                sh = gc.open_by_key(f['id'])
                rangos = ["'Proyección - Ingresos'!A1:Z1000", "'Proyección - Gastos'!A1:Z1000"]
                batch = sh.values_batch_get(rangos)
                
                df_in = limpiar_dataframe_pmo(batch['valueRanges'][0].get('values', []), f['name'], "Ingreso", TRADUCTOR_INGRESOS)
                df_out = limpiar_dataframe_pmo(batch['valueRanges'][1].get('values', []), f['name'], "Gasto", TRADUCTOR_GASTOS)
                
                res = {"in": df_in, "out": df_out}
                with contador_lock:
                    procesados += 1
                    archivos_exitosos.append(f['name']) # Guarda el archivo exitoso
                    print(f"[{procesados}/{total_archivos}] ✅ Completado: {f['name']}")
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e) or "500" in str(e) or "502" in str(e):
                    intentos += 1
                    tiempo_espera = 20 * intentos
                    print(f"⚠️ CUOTA EXCEDIDA en {f['name']}. Reintentando en {tiempo_espera}s... (Intento {intentos}/3)")
                    time.sleep(tiempo_espera)
                else: 
                    print(f"🚨 API Error en {f['name']}: {e}") # Reporta fallos de Google
                    break
            except Exception as e: 
                print(f"🚨 Error fatal inesperado en {f['name']}: {e}") # Reporta fallos de código
                break
        return res

    lista_in, lista_out = [], []
    with ThreadPoolExecutor(max_workers=3) as executor:
        resultados = list(executor.map(worker, files_validos))

    for r in resultados:
        if r:
            if r['in'] is not None: lista_in.append(r['in'])
            if r['out'] is not None: lista_out.append(r['out'])

    if lista_in or lista_out:
        print("\n⚡ Consolidando datos...")
        def union(lista): return pl.concat(lista, how="diagonal") if lista else None
        
        master_in = union(lista_in)
        master_out = union(lista_out)

        if master_in is not None: export_to_drive(gc, master_in, MASTER_SPREADSHEET_ID, "Ingresos")
        if master_out is not None: export_to_drive(gc, master_out, MASTER_SPREADSHEET_ID, "Gastos")
        
        comb = [df for df in [master_in, master_out] if df is not None]
        if comb:
            base_looker = pl.concat(comb, how="diagonal")
            
            cols_looker = [c for c in ORDEN_MAESTRO if c in base_looker.columns]
            base_looker = base_looker.select(cols_looker)
            
            export_to_drive(gc, base_looker, MASTER_SPREADSHEET_ID, "Base_Looker")

        print(f"\n✅ Pipeline Finalizado. Total de archivos procesados: {procesados}/{total_archivos}")
    else:
        print("❌ No se recolectaron datos.")

    # 3️⃣ ALERTA FINAL DE ARCHIVOS NO PROCESADOS
    if procesados < total_archivos:
        todos_los_nombres = [f['name'] for f in files_validos]
        faltantes = set(todos_los_nombres) - set(archivos_exitosos)
        print("\n⚠️ ALERTA - Archivos con error no procesados:")
        for archivo in faltantes:
            print(f"  ❌ {archivo}")

if __name__ == "__main__":
    run_finanzas_pipeline()