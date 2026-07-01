import os
import json
import polars as pl
import gspread
from google.oauth2.credentials import Credentials

# ==============================================================================
# 1. CONFIGURACIÓN DE IDs
# ==============================================================================
ID_BASE_LOOKER = "1vUcnKrp5EfCbW5mh3L76x_UoyB4m9BPhJ_pKHPbxsGM" 
ID_CARPETA_DWH = "1_8cyY32pxRXU3Au0OZOor1wNN7uXO-wr"           
ID_CARPETA_CONSOLIDADO = "1PdRIlCTiwZDxnjUiYXOpklbqWcJgEt5N"   
ID_MAESTRO_PROYECTOS = "1Rx6e85e0vmLAF2SzOEnCl3k3C6VcYG_VRoBpyHxhqqw"

ID_CARPETA_DESTINO_FINAL = "1Mzy21lddSd4JN01DWvolzMKceM48tzeE" 

# ==============================================================================
# 2. AUTENTICACIÓN Y FUNCIONES AUXILIARES
# ==============================================================================
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    token_str = os.environ.get('GOOGLE_OAUTH_TOKEN')
    if token_str:
        return gspread.authorize(Credentials.from_authorized_user_info(json.loads(token_str), scopes))
    return gspread.authorize(Credentials.from_authorized_user_file('token.json', scopes))

def export_to_drive(gc, df: pl.DataFrame, file_name: str, folder_id: str):
    if df.is_empty(): 
        print("⚠️ DataFrame vacío. No hay nada que exportar.")
        return
        
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
    print(f"✅ Guardado con éxito: {file_name}")

def leer_hoja_segura(raw_data):
    """Rellena filas asimétricas y evita nombres de columnas duplicados."""
    if not raw_data or len(raw_data) < 2:
        return pl.DataFrame()
        
    headers = raw_data[0]
    clean_headers = []
    seen = {}
    
    for col in headers:
        col_name = str(col).strip() if str(col).strip() else "Unnamed"
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
        
    return pl.DataFrame(data_rows, schema=clean_headers, orient="row")

# ==============================================================================
# 3. PIPELINE DE UNIFICACIÓN (DATAMART)
# ==============================================================================
def run_unificacion():
    print("🚀 Iniciando creación de la One Big Table (OBT)...")
    gc = get_gspread_client()
    
    # --- 1. LEER ARCHIVOS Y FILTRAR COLUMNAS ---
    print("📥 Leyendo bases de datos y filtrando columnas...")
    
    # 🎯 Finanzas (Actualizado con "Tipo_Movimiento")
    cols_finanzas = ["Proyecto", "Tipo_Movimiento", "Situación", "USD sin impuestos"] 
    sh_finanzas = gc.open_by_key(ID_BASE_LOOKER).worksheet("Base_Looker")
    df_finanzas = leer_hoja_segura(sh_finanzas.get_all_values())
    if not df_finanzas.is_empty():
        presentes = [c for c in cols_finanzas if c in df_finanzas.columns]
        df_finanzas = df_finanzas.select(presentes)

    # 🎯 Presupuestos
    cols_presupuestos = ["Proyecto", "Horas_Presupuestadas", "Costo_Total_Proyecto"]
    archivos_dwh = gc.list_spreadsheet_files(folder_id=ID_CARPETA_DWH)
    id_pres = next((f['id'] for f in archivos_dwh if f['name'] == "Fact_Presupuesto_Horas"), None)
    df_presupuestos = pl.DataFrame()
    if id_pres:
        sh_presupuestos = gc.open_by_key(id_pres).worksheet("Datos")
        df_presupuestos = leer_hoja_segura(sh_presupuestos.get_all_values())
        if not df_presupuestos.is_empty():
            presentes = [c for c in cols_presupuestos if c in df_presupuestos.columns]
            df_presupuestos = df_presupuestos.select(presentes)

    # 🎯 Timesheets
    cols_timesheets = ["Proyecto", "Cantidad de horas", "costo_rate_interno"]
    archivos_cons = gc.list_spreadsheet_files(folder_id=ID_CARPETA_CONSOLIDADO)
    id_time = next((f['id'] for f in archivos_cons if f['name'] == "Productividad Equi Consolidado"), None)
    df_timesheets = pl.DataFrame()
    if id_time:
        sh_timesheets = gc.open_by_key(id_time).worksheet("Datos")
        df_timesheets = leer_hoja_segura(sh_timesheets.get_all_values())
        if not df_timesheets.is_empty():
            presentes = [c for c in cols_timesheets if c in df_timesheets.columns]
            df_timesheets = df_timesheets.select(presentes)

    # --- 2. CREAR LLAVE ROBUSTA Y APLANAR (WIDE FORMAT) ---
    print("🗜️ Aplanando y calculando totales por proyecto...")
    
    # ==============================================================================
    # 🎯 LISTA PARA DEFINIR EL ORDEN DE LAS COLUMNAS DE FINANZAS
    # Modifica el orden de los elementos de esta lista según tu preferencia
    # ==============================================================================
    ORDEN_COLUMNAS_FINANZAS = [
        "join_key",             
        "Ingresos_Reales",
        "Ingresos_Proyectados",
        "Gastos_Reales",
        "Gastos_Proyectados"
    ]

    # Limpieza Finanzas (Ingresos/Gastos separados por Real/Proyectado)
    if not df_finanzas.is_empty() and "Proyecto" in df_finanzas.columns and "USD sin impuestos" in df_finanzas.columns:
        df_finanzas = df_finanzas.with_columns([
            pl.col("Proyecto").cast(pl.Utf8).str.to_uppercase().str.replace_all(r"\s+", " ").str.strip_chars().alias("join_key"),
            
            # ✨ NUEVO: Limpieza robusta de números (elimina puntos de miles, cambia coma por punto)
            pl.col("USD sin impuestos")
            .cast(pl.Utf8)
            .str.replace_all(r"\.", "") # 1. Elimina los puntos (separador de miles)
            .str.replace_all(r"[^0-9,\-]", "") # 2. Deja solo números, comas y signos menos
            .str.replace(",", ".") # 3. Convierte la coma decimal en punto para Python
            .cast(pl.Float64, strict=False)
            .fill_null(0),
            
            pl.col("Situación").cast(pl.Utf8).str.to_uppercase().str.strip_chars().alias("Situación_Limpia"),
            pl.col("Tipo_Movimiento").cast(pl.Utf8).str.to_uppercase().str.strip_chars().alias("Movimiento_Limpio")
        ])
        
        df_finanzas_flat = df_finanzas.group_by("join_key").agg([
            # INGRESOS REALES
            pl.col("USD sin impuestos").filter(
                (pl.col("Movimiento_Limpio").str.contains("INGRESO")) & (pl.col("Situación_Limpia").str.contains("REAL"))
            ).sum().alias("Ingresos_Reales"),
            
            # INGRESOS PROYECTADOS
            pl.col("USD sin impuestos").filter(
                (pl.col("Movimiento_Limpio").str.contains("INGRESO")) & (pl.col("Situación_Limpia").str.contains("PROYECT"))
            ).sum().alias("Ingresos_Proyectados"),
            
            # GASTOS REALES
            pl.col("USD sin impuestos").filter(
                (pl.col("Movimiento_Limpio").str.contains("GASTO")) & (pl.col("Situación_Limpia").str.contains("REAL"))
            ).sum().alias("Gastos_Reales"),
            
            # GASTOS PROYECTADOS
            pl.col("USD sin impuestos").filter(
                (pl.col("Movimiento_Limpio").str.contains("GASTO")) & (pl.col("Situación_Limpia").str.contains("PROYECT"))
            ).sum().alias("Gastos_Proyectados")
        ])
        
        df_finanzas_flat = df_finanzas_flat.select(ORDEN_COLUMNAS_FINANZAS)

    else:
        df_finanzas_flat = pl.DataFrame({col: [] for col in ORDEN_COLUMNAS_FINANZAS})
    
    # Limpieza Presupuestos
    if not df_presupuestos.is_empty() and "Proyecto" in df_presupuestos.columns:
        cols_pres_numeric = [c for c in ["Horas_Presupuestadas", "Costo_Total_Proyecto"] if c in df_presupuestos.columns]
        exprs_pres = [pl.col("Proyecto").cast(pl.Utf8).str.to_uppercase().str.replace_all(r"\s+", " ").str.strip_chars().alias("join_key")]
        for c in cols_pres_numeric:
            exprs_pres.append(pl.col(c).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0))
        
        df_presupuestos = df_presupuestos.with_columns(exprs_pres)
        
        agg_exprs_pres = []
        if "Horas_Presupuestadas" in cols_pres_numeric:
            agg_exprs_pres.append(pl.col("Horas_Presupuestadas").sum().alias("Total_Horas_Presupuestadas"))
        if "Costo_Total_Proyecto" in cols_pres_numeric:
            agg_exprs_pres.append(pl.col("Costo_Total_Proyecto").sum().alias("Presupuesto_Total_Costo"))
            
        df_presupuestos_flat = df_presupuestos.group_by("join_key").agg(agg_exprs_pres) if agg_exprs_pres else pl.DataFrame({"join_key": []})
    else:
        df_presupuestos_flat = pl.DataFrame({"join_key": [], "Total_Horas_Presupuestadas": [], "Presupuesto_Total_Costo": []})

    # Limpieza Timesheets
    if not df_timesheets.is_empty() and "Proyecto" in df_timesheets.columns:
        cols_time_numeric = [c for c in ["Cantidad de horas", "costo_rate_interno"] if c in df_timesheets.columns]
        exprs_time = [pl.col("Proyecto").cast(pl.Utf8).str.to_uppercase().str.replace_all(r"\s+", " ").str.strip_chars().alias("join_key")]
        for c in cols_time_numeric:
            exprs_time.append(pl.col(c).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0))
            
        df_timesheets = df_timesheets.with_columns(exprs_time)
        
        agg_exprs_time = []
        if "Cantidad de horas" in cols_time_numeric:
            agg_exprs_time.append(pl.col("Cantidad de horas").sum().alias("Total_Horas_Ejecutadas"))
        if "costo_rate_interno" in cols_time_numeric:
            agg_exprs_time.append(pl.col("costo_rate_interno").sum().alias("Costo_Real_Ejecutado"))
            
        df_timesheets_flat = df_timesheets.group_by("join_key").agg(agg_exprs_time) if agg_exprs_time else pl.DataFrame({"join_key": []})
    else:
        df_timesheets_flat = pl.DataFrame({"join_key": [], "Total_Horas_Ejecutadas": [], "Costo_Real_Ejecutado": []})

    # --- 3. UNIR TODO (OUTER JOIN) ---
    print("🔗 Ensamblando el Datamart Único...")
    maestro_final = df_finanzas_flat.join(df_presupuestos_flat, on="join_key", how="outer", coalesce=True)
    maestro_final = maestro_final.join(df_timesheets_flat, on="join_key", how="outer", coalesce=True)
    
    # Rellenamos los nulos con 0 para evitar errores matemáticos en Looker
    for col in maestro_final.columns:
        if col != "join_key":
            maestro_final = maestro_final.with_columns(pl.col(col).fill_null(0))

    # ==============================================================================
    # ✨ NUEVAS COLUMNAS CALCULADAS (Márgenes y Diferencias)
    # ==============================================================================
    maestro_final = maestro_final.with_columns([
        # Diferencias (Actual - Forecast)
        (pl.col("Ingresos_Reales") - pl.col("Ingresos_Proyectados")).alias("Dif_Ingresos_Real_vs_Proy"),
        (pl.col("Gastos_Reales") - pl.col("Gastos_Proyectados")).alias("Dif_Gastos_Real_vs_Proy"),
        
        # Margen de Contribución Forecast (Ingresos - Gastos - Costos Presupuestados)
        (pl.col("Ingresos_Proyectados") - pl.col("Gastos_Proyectados") - pl.col("Presupuesto_Total_Costo")).alias("Margen_Contribucion_Proyectado"),
        
        # Margen de Contribución Actual (Ingresos - Gastos - Costos Ejecutados)
        (pl.col("Ingresos_Reales") - pl.col("Gastos_Reales") - pl.col("Costo_Real_Ejecutado")).alias("Margen_Contribucion_Real")
    ])
    
    # --- 4. TRAER LOS FILTROS DEL MAESTRO DE PROYECTOS ---
    print("📚 Adjuntando filtros globales del Maestro...")
    try:
        sh_maestro = gc.open_by_key(ID_MAESTRO_PROYECTOS).worksheet("Proyectos")
        raw_maestro = sh_maestro.get_all_values()
        if raw_maestro and len(raw_maestro) > 1:
            df_m = leer_hoja_segura(raw_maestro)
            
            df_filtros = df_m.with_columns(
                pl.col("Proyecto").cast(pl.Utf8).str.to_uppercase().str.replace_all(r"\s+", " ").str.strip_chars().alias("join_key")
            ).select([
                "join_key",
                pl.col("Proyecto").alias("Nombre_Proyecto_Original"), 
                pl.col("Tipo de proyecto").alias("Tipo_de_proyecto"),
                pl.col("País de facturación proyecto").alias("Pais_Facturacion_Proyecto"),
                pl.col("Activo").alias("Activo")
            ]).unique(subset=["join_key"])
            
            maestro_final = maestro_final.join(df_filtros, on="join_key", how="left")
    except Exception as e:
        print(f"🚨 Error al adjuntar filtros: {e}")

    # Limpieza final de columnas (recuperar nombre legible del proyecto)
    if "Nombre_Proyecto_Original" in maestro_final.columns:
        maestro_final = maestro_final.with_columns(
            pl.coalesce(["Nombre_Proyecto_Original", "join_key"]).alias("Proyecto")
        ).drop(["join_key", "Nombre_Proyecto_Original"])
    else:
        maestro_final = maestro_final.rename({"join_key": "Proyecto"})
        
    # Reordenar para que "Proyecto" quede de primero
    cols_order = ["Proyecto"] + [c for c in maestro_final.columns if c != "Proyecto"]
    maestro_final = maestro_final.select(cols_order)

    # --- 5. EXPORTAR ---
    print(f"📤 Exportando Maestro_Looker_Studio ({len(maestro_final)} proyectos únicos)...")
    export_to_drive(gc, maestro_final, "Maestro_Looker_Studio", ID_CARPETA_DESTINO_FINAL)
    print("🎉 Proceso finalizado exitosamente.")

if __name__ == "__main__":
    run_unificacion()