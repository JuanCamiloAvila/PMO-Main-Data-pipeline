### script ingresos y gastos V2

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
FOLDER_IDS_PRESUPUESTOS = ["1uFZXGHpfab4iL-mvmzH0i5boNphhZHEj"]
MASTER_SPREADSHEET_ID = "1vUcnKrp5EfCbW5mh3L76x_UoyB4m9BPhJ_pKHPbxsGM"

# Nombres de pestañas - Formato nuevo (archivos "Monitoreo...")
PESTANA_MIXTA = "Proyección Ingresos - Gastos"
PESTANA_MONITOREO_IN = "Monitoreo Ingresos"
PESTANA_MONITOREO_OUT = "Monitoreo Gastos"

# 1. ORDEN_MAESTRO (Sin proyecto_id)
ORDEN_MAESTRO = [
    "archivo_origen", "Tipo_Movimiento", "Fecha", "Proyecto", "País de facturación", 
    "Categoría", "Tipo de gasto", "Descripción", "Producto/Entregable/Servicio", 
    "Monto sin Impuestos", "IGV/IVA/Otros", "Monto con Impuestos", 
    "Moneda", "TC", "USD con impuestos", "USD sin impuestos", "Fecha de entrega del producto", 
    "Fecha de emisión del comprobante", "Situación", "Fecha de factura proveedor"
]

TRADUCTOR_INGRESOS = {
    "Proyecto / Cuenta analítica" : "Proyecto",
    "2" : "Proyecto",
    "USD" : "USD con impuestos",
    "USD (sin impuestos)": "USD sin impuestos",
    "Tipo de movimiento": "Situación"
}

# 2. COLUMNAS_INGRESOS (Sin proyecto_id)
COLUMNAS_INGRESOS = [
    "Fecha", "Proyecto", "País de facturación", "Producto/Entregable/Servicio", 
    "Monto sin Impuestos", "IGV/IVA/Otros", "Monto con Impuestos", 
    "Moneda", "TC", "USD con impuestos", "USD sin impuestos", "Fecha de entrega del producto", 
    "Fecha de emisión del comprobante", "Situación"
]

TRADUCTOR_GASTOS = {
    "Monto Total / (Monto sin Impuestos)": "Monto sin Impuestos",
    "SItuación": "Situación",
    "USD" : "USD con impuestos",
    "USD (sin impuestos)": "USD sin impuestos",
    "Tipo de movimiento": "Situación"
}

# Traductor para pestaña mixta (Proyección Ingresos - Gastos)
TRADUCTOR_MIXTO = {
    "Monto Total / (Monto sin Impuestos)": "Monto sin Impuestos",
    "USD" : "USD con impuestos",
    "USD (sin impuestos)": "USD sin impuestos",
    "Ingreso o Gasto": "Tipo_Movimiento"
}

# 3. COLUMNAS_GASTOS (Sin proyecto_id)
COLUMNAS_GASTOS = [ 
    "Fecha", "Proyecto", "País de facturación", "Categoría", "Tipo de gasto", 
    "Descripción", "Fecha de factura proveedor", "Monto sin Impuestos", 
    "IGV/IVA/Otros", "Monto con Impuestos", "Moneda", "TC", "USD con impuestos", "USD sin impuestos", "Situación"
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
        print(f"⚠️ ATENCIÓN: No hay datos para exportar a la pestaña '{tab_name}'. Se omitirá este paso.")
        return
    
    datos_exportar = [list(df.columns)]
    for row in df.rows():
        datos_exportar.append(["" if val is None else val for val in row])
    
    intentos, exito = 0, False
    # 🔥 CAMBIADO: De 3 a 6 intentos en la exportación
    while intentos < 6 and not exito:
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
            # 🔥 Agregados 500, 502, 503 y 504 a la exportación
            if any(err in str(e) for err in ["429", "500", "502", "503", "504"]):
                intentos += 1
                time.sleep(20 * intentos)

def limpiar_dataframe_pmo(raw_rows, file_name, tipo, traductor, usar_columna_tipo=False, solo_real=False):
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
            col_limpia = col_upper.replace('\n', ' ').replace('\r', ' ').strip()
            while "  " in col_limpia: col_limpia = col_limpia.replace("  ", " ")
            
            objetivo = traductor.get(col_real) if traductor else None
            
            if not objetivo:
                if col_limpia.startswith("PROYECTO"): 
                    objetivo = "Proyecto"
                elif col_real == "2" or col_limpia.startswith("MONTO SIN"): 
                    objetivo = "Monto sin Impuestos"
                elif col_limpia.startswith("MONTO CON"): 
                    objetivo = "Monto con Impuestos"
                elif col_limpia.startswith("SITUACI"): 
                    objetivo = "Situación"
                elif "USD" in col_limpia and "SIN IMPUESTOS" in col_limpia:
                    objetivo = "USD sin impuestos"
                elif col_limpia == "USD" or ("USD" in col_limpia and "CON IMPUESTOS" in col_limpia):
                    objetivo = "USD con impuestos"
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
        if usar_columna_tipo:
            if "Fecha de factura proveedor" in df.columns:
                df = df.with_columns(pl.col("Fecha de factura proveedor").alias("Fecha"))
            else:
                df = df.with_columns(pl.lit("").alias("Fecha"))
        elif tipo == "Ingreso" and "Fecha de emisión del comprobante" in df.columns:
            df = df.with_columns(pl.col("Fecha de emisión del comprobante").alias("Fecha"))
        elif tipo == "Gasto" and "Fecha de factura proveedor" in df.columns:
            df = df.with_columns(pl.col("Fecha de factura proveedor").alias("Fecha"))
        else:
            df = df.with_columns(pl.lit("").alias("Fecha"))
            
        if usar_columna_tipo:
            df = df.with_columns(pl.lit("Proyectado").alias("Situación"))

        # --- 🧹 4. PURGA DE FILAS FANTASMA ---
        columnas_clave = [c for c in ["Proyecto", "Situación", "Monto con Impuestos", "Descripción", "Fecha"] if c in df.columns]
        if columnas_clave:
            condicion_datos_reales = pl.lit(False)
            for c in columnas_clave:
                condicion_datos_reales = condicion_datos_reales | (pl.col(c).cast(pl.Utf8).str.strip_chars() != "")
            df = df.filter(condicion_datos_reales)

        total_inicial = len(df) 
        
        # --- 🛡️ 5. FILTROS ESTRICTOS FINANCIEROS Y LIMPIEZA NUMÉRICA ---
        if "Situación" in df.columns:
            if solo_real:
                # FÁCIL REVERSIÓN: Si dirección no lo acepta, elimina esta condición if/else y deja solo la lista con ["REAL", "PROYECTADO"].
                df = df.filter(pl.col("Situación").cast(pl.Utf8).str.to_uppercase().str.strip_chars().is_in(["REAL"]))
            else:
                df = df.filter(pl.col("Situación").cast(pl.Utf8).str.to_uppercase().str.strip_chars().is_in(["REAL", "PROYECTADO"]))

        # Limpieza numérica de las nuevas columnas USD
        tiene_filtro_usd = False
        condicion_usd = pl.lit(False)
        for col_usd in ["USD con impuestos", "USD sin impuestos"]:
            if col_usd in df.columns:
                df = df.with_columns(
                    pl.col(col_usd).cast(pl.Utf8)
                    .str.replace_all(r"[^0-9,.]", "")
                    .str.replace_all(r"\.", "")
                    .str.replace(",", ".")
                    .alias(col_usd)
                )
                df = df.with_columns(pl.col(col_usd).cast(pl.Float64, strict=False))
                
                # Preparamos el filtro: al menos una de las columnas debe tener un monto válido > 0
                condicion_usd = condicion_usd | (pl.col(col_usd).is_not_null() & (pl.col(col_usd) > 0))
                tiene_filtro_usd = True
                
        if tiene_filtro_usd:
            df = df.filter(condicion_usd)

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

        # --- 7. SELECCIÓN FINAL Y EXTRACCIÓN POR DÍGITO ---
        if usar_columna_tipo:
            cols_finales = list(dict.fromkeys(COLUMNAS_INGRESOS + COLUMNAS_GASTOS)) + ["Tipo_Movimiento"]
        else:
            cols_finales = COLUMNAS_INGRESOS if tipo == "Ingreso" else COLUMNAS_GASTOS
        presentes = [c for c in cols_finales if c in df.columns]
        
        if df.is_empty() or not presentes: return None

        # Extraemos el nombre del proyecto: el formato es
        # "Monitoreo - [código] - [Empresa] - [Nombre del proyecto]"
        # Dividimos por " - " y eliminamos SOLO el primer segmento ("Monitoreo").
        # Tomamos partes[1:] para conservar: código + empresa + nombre completo.
        # Esto preserva "Monitoreo" si forma parte del nombre del proyecto.
        nombre_sin_ext = file_name.split(".")[0]  # Quitamos extensión primero
        partes = nombre_sin_ext.split(" - ")
        if len(partes) >= 2:
            # Quitamos solo el primer segmento ("Monitoreo") y reunimos el resto
            nombre_proyecto_estandar = " - ".join(partes[1:]).strip()
        else:
            # Último recurso: usamos el nombre completo sin extensión
            nombre_proyecto_estandar = nombre_sin_ext.strip()

        columnas_meta = [
            pl.lit(file_name).alias("archivo_origen"),
            pl.lit(nombre_proyecto_estandar).alias("Proyecto")
        ]
        
        if not usar_columna_tipo:
            columnas_meta.append(pl.lit(tipo).alias("Tipo_Movimiento"))

        return df.select(presentes).with_columns(columnas_meta)

    # ESTE ES EL BLOQUE QUE FALTABA
    except Exception as e:
        print(f"❌ Error estructurando {file_name} ({tipo}): {e}")
        return None

        
# ==============================================================================
# 3. EJECUCIÓN
# ==============================================================================
def run_finanzas_pipeline():
    print("🚀 Iniciando Híbrido Maestro (Multithreading + Batching + Auditoría)...")
    gc = get_gspread_client()
    
    from googleapiclient.discovery import build
    
    # Reconstruimos las credenciales para acceder a la API de Drive directamente
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    token_str = os.environ.get('GOOGLE_OAUTH_TOKEN')
    if token_str:
        creds = Credentials.from_authorized_user_info(json.loads(token_str), scopes)
    else:
        creds = Credentials.from_authorized_user_file('token.json', scopes)
        
    drive_service = build('drive', 'v3', credentials=creds)

    files = []
    for f_url in FOLDER_IDS_PRESUPUESTOS:
        f_id = f_url.split('/folders/')[-1].split('?')[0].strip()
        query = f"'{f_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        
        page_token = None
        while True:
            # 1. Agregamos soporte para Unidades Compartidas y pedimos trashed/parents explícitamente
            res_drive = drive_service.files().list(
                q=query, 
                fields="nextPageToken, files(id, name, trashed, parents)",
                supportsAllDrives=True, 
                includeItemsFromAllDrives=True,
                pageToken=page_token
            ).execute()
            
            # 2. Doble validación estricta en Python (Bypass al caché de Google)
            for archivo in res_drive.get('files', []):
                esta_en_papelera = archivo.get('trashed', False)
                padres_actuales = archivo.get('parents', [])
                
                # Solo aceptamos el archivo si NO está en papelera y f_id SIGUE siendo su padre real
                if not esta_en_papelera and f_id in padres_actuales:
                    files.append(archivo)
            
            # 3. Paginación: Asegura que lea todos los archivos si hay más de 100
            page_token = res_drive.get('nextPageToken')
            if not page_token:
                break
    
    files_validos = [f for f in {fi['id']: fi for fi in files}.values() 
                     if f['name'].upper().startswith("MONITOREO") 
                     and "COPIA" not in f['name'].upper()]

    total_archivos = len(files_validos)
    procesados = 0
    contador_lock = threading.Lock()
    
    archivos_exitosos = []

    def worker(f):
        nonlocal procesados
        res = None
        intentos = 0
        # 🔥 CAMBIADO: De 3 a 6 intentos en la lectura de archivos
        while intentos < 6:
            try:
                sh = gc.open_by_key(f['id'])
                
                rangos = [
                    f"'{PESTANA_MIXTA}'!A:Z",
                    f"'{PESTANA_MONITOREO_IN}'!A:Z",
                    f"'{PESTANA_MONITOREO_OUT}'!A:Z"
                ]
                batch = sh.values_batch_get(rangos)
                
                df_mixta = limpiar_dataframe_pmo(batch['valueRanges'][0].get('values', []), f['name'], "Mixto", TRADUCTOR_MIXTO, usar_columna_tipo=True)
                # FÁCIL REVERSIÓN: Quitar 'solo_real=True' si se desea permitir de nuevo extraer 'Proyectado' en las pestañas de Monitoreo
                df_mon_in = limpiar_dataframe_pmo(batch['valueRanges'][1].get('values', []), f['name'], "Ingreso", TRADUCTOR_INGRESOS, solo_real=True)
                df_mon_out = limpiar_dataframe_pmo(batch['valueRanges'][2].get('values', []), f['name'], "Gasto", TRADUCTOR_GASTOS, solo_real=True)
                
                res = {"in": df_mon_in, "out": df_mon_out, "mixta": df_mixta}
                
                with contador_lock:
                    procesados += 1
                    archivos_exitosos.append(f['name'])
                    print(f"[{procesados}/{total_archivos}] ✅ [NUEVO] {f['name']}")
                break
            except gspread.exceptions.APIError as e:
                if any(err in str(e) for err in ["429", "500", "502", "503", "504"]):
                    intentos += 1
                    tiempo_espera = 20 * intentos
                    # 🔥 CAMBIADO: El mensaje ahora refleja "(Intento X/6)"
                    print(f"⚠️ CUOTA EXCEDIDA en {f['name']}. Reintentando en {tiempo_espera}s... (Intento {intentos}/6)")
                    time.sleep(tiempo_espera)
                else: 
                    print(f"🚨 API Error en {f['name']}: {e}") 
                    break
            except Exception as e: 
                print(f"🚨 Error fatal inesperado en {f['name']}: {e}") 
                break
        return res

    lista_in, lista_out, lista_mixta = [], [], []
    with ThreadPoolExecutor(max_workers=3) as executor:
        resultados = list(executor.map(worker, files_validos))

    for r in resultados:
        if r:
            if r.get('in') is not None: lista_in.append(r['in'])
            if r.get('out') is not None: lista_out.append(r['out'])
            if r.get('mixta') is not None: lista_mixta.append(r['mixta'])

    base_looker = None
    if lista_in or lista_out or lista_mixta:
        print("\n⚡ Consolidando datos...")
        def union(lista): 
            if not lista: return None
            df = pl.concat(lista, how="diagonal")
            # Forzar el orden maestro para que todas las pestañas se vean iguales
            cols = [c for c in ORDEN_MAESTRO if c in df.columns]
            return df.select(cols)
        
        master_in = union(lista_in)
        master_out = union(lista_out)
        master_mixta = union(lista_mixta)

        if master_in is not None: export_to_drive(gc, master_in, MASTER_SPREADSHEET_ID, "Ingresos")
        if master_out is not None: export_to_drive(gc, master_out, MASTER_SPREADSHEET_ID, "Gastos")
        if master_mixta is not None: export_to_drive(gc, master_mixta, MASTER_SPREADSHEET_ID, "Proyección")
        
        comb = [df for df in [master_in, master_out, master_mixta] if df is not None]
        if comb:
            base_looker = pl.concat(comb, how="diagonal")
            
            # Filtramos columnas para mantener solo las de ORDEN_MAESTRO
            cols_looker = [c for c in ORDEN_MAESTRO if c in base_looker.columns]
            base_looker = base_looker.select(cols_looker)
            
            print(f"📊 Total registros consolidados en Base_Looker: {len(base_looker)} (Ingresos + Gastos)")
            
            # =========================================================
            # 📤 EXPORTACIÓN AL MASTER SPREADSHEET
            # =========================================================
            print("📤 Exportando Base_Looker consolidada...")
            export_to_drive(gc, base_looker, MASTER_SPREADSHEET_ID, "Base_Looker")

            print(f"\n✅ Pipeline Finalizado.")
            
        else:
            print("❌ CRÍTICO: No se recolectaron datos. Revisa tus filtros o los archivos en Drive.")
            sys.exit(1)

        if procesados < total_archivos:
            todos_los_nombres = [f['name'] for f in files_validos]
            faltantes = set(todos_los_nombres) - set(archivos_exitosos)
            print("\n⚠️ ALERTA - Archivos con error no procesados:")
            for archivo in faltantes:
                print(f"  ❌ {archivo}")

if __name__ == "__main__":
    try:
        run_finanzas_pipeline()
    except Exception as e:
        print("\n❌ Error crítico de ejecución:")
        traceback.print_exc()
        sys.exit(1)  
