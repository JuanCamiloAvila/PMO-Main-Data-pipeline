import os
import json
import polars as pl
import gspread
from google.oauth2.credentials import Credentials
import traceback
import time

# ==============================================================================
# 1. CONFIGURACIÓN DE IDs Y DICCIONARIOS
# ==============================================================================
# 
FOLDER_IDS_PRESUPUESTOS = ["18Ma7mj63Egs_KfyLtkdOPQnvBs6__aGw"]
MASTER_SPREADSHEET_ID = "1vUcnKrp5EfCbW5mh3L76x_UoyB4m9BPhJ_pKHPbxsGM"

# Orden que queremos ver en el Excel final (puedes moverlos aquí si quieres otro orden)
ORDEN_MAESTRO = [
    "Proyecto", "País de facturación", "Producto/Entregable/Servicio", 
    "Monto sin Impuestos", "IGV/IVA/Otros", "Monto con Impuestos", 
    "Moneda", "TC", "USD", "Fecha de entrega del producto", 
    "Fecha de emisión del comprobante", "Situación", 
    "Tipo_Movimiento", "archivo_origen", "Categoría", "Tipo de gasto", "Descripción"
]

TRADUCTOR_INGRESOS = {"Proyecto / Cuenta analítica": "Proyecto"}
COLUMNAS_INGRESOS = [
    "Proyecto", "País de facturación", "Producto/Entregable/Servicio", 
    "Monto sin Impuestos", "IGV/IVA/Otros", "Monto con Impuestos", 
    "Moneda", "TC", "USD", "Fecha de entrega del producto", 
    "Fecha de emisión del comprobante", "Situación"
]

TRADUCTOR_GASTOS = {
    "Monto Total / (Monto sin Impuestos)": "Monto sin Impuestos",
    "SItuación": "Situación"
}
COLUMNAS_GASTOS = [
    "Proyecto", "País de facturación", "Categoría", "Tipo de gasto", 
    "Descripción", "Fecha de factura proveedor", "Monto sin Impuestos", 
    "IGV/IVA/Otros", "Monto con Impuestos", "Moneda", "TC", "USD", "Situación"
]

# ==============================================================================
# 2. FUNCIONES DE APOYO
# ==============================================================================
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    token_str = os.environ.get('GOOGLE_OAUTH_TOKEN')
    if token_str:
        return gspread.authorize(Credentials.from_authorized_user_info(json.loads(token_str), scopes))
    return gspread.authorize(Credentials.from_authorized_user_file('token.json', scopes))

def safe_read_sheet(sheet) -> pl.DataFrame:
    raw_data = sheet.get_all_values()
    if not raw_data or len(raw_data) < 2: return None
    clean_headers = [col.strip() if col.strip() else f"Unnamed_{i}" for i, col in enumerate(raw_data[0])]
    num_cols = len(clean_headers)
    data_rows = [row + [""] * (num_cols - len(row)) for row in raw_data[1:]]
    return pl.DataFrame(data_rows, schema=clean_headers, orient="row").with_columns(pl.all().cast(pl.Utf8))

def export_to_drive(gc, df: pl.DataFrame, file_id: str, tab_name: str):
    if df is None or df.is_empty(): return
    
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
            time.sleep(3)
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                intentos += 1
                time.sleep(20 * intentos)
            else: raise e

# ==============================================================================
# 3. LÓGICA DE EXTRACCIÓN Y LIMPIEZA
# ==============================================================================
def extraer_datos_finanzas(wb, file_name, tab_name, traductor, columnas_finales, tipo_movimiento):
    try:
        sheet = wb.worksheet(tab_name)
        df = safe_read_sheet(sheet)
        if df is None: return None
        
        # 1. Limpieza inicial de nombres (quitar espacios locos)
        df = df.rename({col: col.strip() for col in df.columns})
        
        # 2. 🧠 MAPEADO INTELIGENTE (Solución al problema de la imagen)
        mapeo_dinamico = {}
        for col_real in df.columns:
            col_upper = col_real.upper()
            
            # Si la columna empieza con "PROYECTO", la mapeamos a "Proyecto"
            if col_upper.startswith("PROYECTO"):
                mapeo_dinamico[col_real] = "Proyecto"
            # Si la columna es "2" o empieza con "MONTO SIN", la mapeamos
            elif col_real == "2" or col_upper.startswith("MONTO SIN"):
                mapeo_dinamico[col_real] = "Monto sin Impuestos"
            # Si empieza con "SITUACIÓN" o "SITUACION" (por las tildes)
            elif col_upper.startswith("SITUACI"):
                mapeo_dinamico[col_real] = "Situación"
        
        # Aplicamos el mapeo dinámico y el traductor manual que ya teníamos
        df = df.rename(mapeo_dinamico)
        
        # Por si quedaron otras traducciones manuales en el diccionario original
        rename_rest = {v: n for v, n in traductor.items() if v in df.columns and v not in mapeo_dinamico}
        if rename_rest: df = df.rename(rename_rest)
            
        # --- AUDITORÍA ---
        columnas_criticas = ["Proyecto", "Monto sin Impuestos", "Monto con Impuestos"]
        for crit in columnas_criticas:
            if crit not in df.columns:
                print(f"   ⚠️ ALERTA en '{file_name}': No se encontró '{crit}'.")
                print(f"      Columnas encontradas: {list(df.columns[:5])}...")
        
        # 3. Seleccionar columnas finales
        cols_presentes = [col for col in columnas_finales if col in df.columns]
        df = df.select(cols_presentes)
        
        df = df.with_columns([
            pl.lit(tipo_movimiento).alias("Tipo_Movimiento"),
            pl.lit(file_name).alias("archivo_origen")
        ])
        return df
    except gspread.exceptions.WorksheetNotFound:
        return None

def limpiar_y_ordenar(lista_dfs):
    if not lista_dfs: return None
    df = pl.concat(lista_dfs, how="diagonal")
    
    # Filtro: Solo filas donde "Monto con Impuestos" NO sea cero o vacío
    if "Monto con Impuestos" in df.columns:
        df = df.filter(
            (pl.col("Monto con Impuestos").cast(pl.Utf8).str.strip_chars() != "") & 
            (pl.col("Monto con Impuestos").cast(pl.Utf8).str.strip_chars() != "0") &
            (pl.col("Monto con Impuestos").is_not_null())
        )
    
    # Reordenar según el ORDEN_MAESTRO
    cols_finales = [c for c in ORDEN_MAESTRO if c in df.columns]
    # Añadir columnas faltantes si se requiere (archivo_origen, etc)
    for extra in ["Tipo_Movimiento", "archivo_origen"]:
        if extra in df.columns and extra not in cols_finales:
            cols_finales.append(extra)
            
    return df.select(cols_finales)

# ==============================================================================
# 4. EJECUCIÓN PRINCIPAL
# ==============================================================================
def run_finanzas_pipeline():
    print("🚀 Iniciando Pipeline de Finanzas...")
    try:
        gc = get_gspread_client()
        files = []
        for f_url in FOLDER_IDS_PRESUPUESTOS:
            f_id = f_url.split('/folders/')[-1].split('?')[0].strip()
            files.extend(gc.list_spreadsheet_files(folder_id=f_id))
        
        # --- AQUÍ VA EL BLOQUE NUEVO ---
        all_files = list({f['id']: f for f in files}.values())
        
        # Filtro inteligente: acepta "NUEVO" o "Nuevo" y descarta las "Copias"
        files_validos = [
            f for f in all_files 
            if f['name'].upper().startswith("NUEVO") 
            and "COPIA" not in f['name'].upper()
        ]
        
        print(f"📊 Resumen de archivos:")
        print(f"   - Encontrados en carpeta: {len(all_files)}")
        print(f"   - Válidos para procesar: {len(files_validos)}")
        print(f"   - Excluidos (Templates/Otros): {len(all_files) - len(files_validos)}\n")

        lista_in, lista_out = [], []
        
        for f in files_validos:
            print(f"🔎 Procesando: {f['name']}...")
            exito_archivo = False
            intentos_cuota = 0
            
            while not exito_archivo:
                try:
                    wb = gc.open_by_key(f['id'])
                    # Cambia aquí los nombres si tus pestañas de origen se llaman distinto:
                    df_in = extraer_datos_finanzas(wb, f['name'], "Proyección - Ingresos", TRADUCTOR_INGRESOS, COLUMNAS_INGRESOS, "Ingreso")
                    df_out = extraer_datos_finanzas(wb, f['name'], "Proyección - Gastos", TRADUCTOR_GASTOS, COLUMNAS_GASTOS, "Gasto")
                    
                    if df_in is not None: lista_in.append(df_in)
                    if df_out is not None: lista_out.append(df_out)
                    
                    exito_archivo = True
                    time.sleep(1.8) 
                except gspread.exceptions.APIError as e:
                    if "429" in str(e):
                        intentos_cuota += 1
                        print(f"  ⏳ Cuota llena. Esperando {15 * intentos_cuota}s...")
                        time.sleep(15 * intentos_cuota)
                    else: break
                except Exception: break

        print("\n⚡ Aplicando limpieza y ordenando columnas...")
        master_in = limpiar_y_ordenar(lista_in)
        master_out = limpiar_y_ordenar(lista_out)

        print("📤 Exportando al Master...")
        if master_in is not None: export_to_drive(gc, master_in, MASTER_SPREADSHEET_ID, "Ingresos")
        if master_out is not None: export_to_drive(gc, master_out, MASTER_SPREADSHEET_ID, "Gastos")
        
        # Generar Base_Looker (unión de ambos)
        comb = [df for df in [master_in, master_out] if df is not None]
        if comb:
            export_to_drive(gc, pl.concat(comb, how="diagonal"), MASTER_SPREADSHEET_ID, "Base_Looker")

        print("\n✅ ¡Proceso finalizado con éxito!")
    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    run_finanzas_pipeline()