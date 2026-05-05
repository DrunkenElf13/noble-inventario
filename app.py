import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime, timezone, timedelta, date as _date
import time
import calendar
import unicodedata
import re
import io
import threading
import time as _time

try:
    from reportlab.lib.pagesizes import landscape
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib.units import mm
    REPORTLAB_OK = True
except ImportError:
    REPORTLAB_OK = False

st.set_page_config(layout="wide")

# ── KEEPALIVE ────────────────────────────────────────────────
def _keepalive_thread(intervalo_seg: int = 120):
    while True:
        _time.sleep(intervalo_seg)
        _ = _time.time()

def iniciar_keepalive(intervalo_seg: int = 120):
    if not st.session_state.get("_keepalive_iniciado", False):
        hilo = threading.Thread(
            target=_keepalive_thread,
            args=(intervalo_seg,),
            daemon=True,
            name="streamlit-keepalive"
        )
        hilo.start()
        st.session_state["_keepalive_iniciado"] = True

iniciar_keepalive(intervalo_seg=120)

# ============================================================
# CONSTANTES
# ============================================================
COLS_INSUMOS = [
    "Unidad de Negocio", "Nombre del Insumo", "Marca", "Proveedor", "Grupo",
    "Espacio_1", "Presentación de Compra", "Unidad de Medida",
    "Espacio_2", "Espacio_3", "Espacio_4", "Stock Mínimo",
    "Espacio_5", "Espacio_6", "Espacio_7", "Espacio_8", "Tara", "Activo",  # ← NUEVO: "Activo" en columna R (índice 17)
]
COLS_HISTORIAL = [
    "Unidad de Negocio", "Nombre del Insumo", "Marca", "Proveedor", "Grupo",
    "Fecha de Entrada", "Presentación de Compra", "Unidad de Medida",
    "Alm", "Barra", "Stock Neto", "Stock Mínimo", "¿Comprar?",
    "Responsable", "Fecha de Inventario", "Tara", "Observaciones",
]
COLS_ACCESOS = ["Clave", "Nombre", "Rol"]
COLS_AVISOS  = ["ID", "Título", "Mensaje", "Tipo", "Activo", "Fecha", "Autor"]
COLS_VENTAS  = [
    "Unidad", "Fecha", "Día", "Mes", "Año",
    "Efectivo", "Transferencias", "Tarjeta", "Total_POS",
    "Uber_Eats", "Rappi", "Venta_Diaria",
    "Tickets_POS", "Tickets_Uber", "Tickets_Rappi", "Total_Tickets",
    "Ticket_Promedio", "Meta_Mensual", "Dias_Habiles", "Meta_Diaria",
    "Responsable", "Notas",
]

COLS_CRITICAS_INSUMOS   = {"Nombre del Insumo", "Grupo", "Stock Mínimo"}
COLS_CRITICAS_HISTORIAL = {"Nombre del Insumo", "Alm", "Barra", "Fecha de Inventario"}

GRUPOS       = ["A", "B", "C", "D", "E", "F", "G"]
UNIDADES     = ["Noble", "Coffee Station"]
UNIDADES_MED = ["pz", "ml", "gr", "kg", "lt"]

SPREADSHEET_ID = "1VZV81p-JqoaRPzMzsRurF6wntVefyaN5ozs3RJe6uJs"

# ============================================================
# ZONA HORARIA — Hermosillo MST UTC-7
# ============================================================
TZ_HERMOSILLO = timezone(timedelta(hours=-7))

def ahora_hermosillo() -> datetime:
    return datetime.now(tz=TZ_HERMOSILLO)

def ts_hermosillo() -> str:
    return ahora_hermosillo().strftime("%Y-%m-%d %H:%M:%S")

def fmt_fecha_hmo(dt) -> str:
    if dt is None or (hasattr(dt, 'isnull') and dt.isnull()):
        return ""
    try:
        import pandas as pd
        if pd.isnull(dt):
            return ""
    except Exception:
        pass
    try:
        if hasattr(dt, 'tzinfo') and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_hmo = dt.astimezone(TZ_HERMOSILLO)
        return dt_hmo.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(dt)[:16]

# ============================================================
# HELPERS UTILITARIOS
# ============================================================
def limpiar_valor(valor) -> float:
    if valor is None:
        return 0.0
    if isinstance(valor, bool):
        return 1.0 if valor else 0.0
    if isinstance(valor, (int, float)):
        try:
            return 0.0 if pd.isna(valor) else float(valor)
        except (TypeError, ValueError):
            return 0.0
    try:
        s = str(valor).strip()
        if not s or s in ("-", "—", "–", "N/A", "n/a", "NA", "na", "None", "null"):
            return 0.0
        s = s.replace('%','').replace('$','').replace(',','').replace(' ','')
        return float(s)
    except (ValueError, TypeError):
        return 0.0

def normalizar_nombre(nombre) -> str:
    s = str(nombre).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'\s+', ' ', s)
    return s

def normalizar_dataframe(df: pd.DataFrame, columnas_esperadas: list,
                         cols_criticas: set = None) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=columnas_esperadas)
    df = df.copy()
    cols_en_sheet  = set(df.columns)
    cols_faltantes = [c for c in columnas_esperadas if c not in cols_en_sheet]
    if cols_criticas:
        faltantes_criticas = set(cols_faltantes) & cols_criticas
        if faltantes_criticas:
            st.warning(
                f"⚠️ Columnas críticas no encontradas en el Sheet: {sorted(faltantes_criticas)}."
            )
    for col in cols_faltantes:
        df[col] = None
    return df[columnas_esperadas]

def safe_worksheet(sh, nombre: str):
    if sh is None:
        return None, "Sin conexión activa a Google Sheets."
    try:
        return sh.worksheet(nombre), None
    except gspread.exceptions.WorksheetNotFound:
        return None, f"Pestaña '{nombre}' no encontrada en el Spreadsheet."
    except Exception as e:
        return None, f"Error accediendo a '{nombre}': {e}"

def append_rows_con_retry(worksheet, filas: list, max_intentos: int = 3) -> tuple:
    if not filas:
        return False, "No hay filas para escribir."
    for intento in range(1, max_intentos + 1):
        try:
            worksheet.append_rows(filas, value_input_option="USER_ENTERED")
            return True, f"{len(filas)} fila(s) registrada(s)."
        except gspread.exceptions.APIError as e:
            codigo = getattr(e.response, 'status_code', 0)
            if codigo == 429 and intento < max_intentos:
                time.sleep(2 ** intento)
                continue
            return False, f"Error de API Sheets (intento {intento}/{max_intentos}): {e}"
        except Exception as e:
            return False, f"Error inesperado al escribir en Sheets: {e}"
    return False, "Se agotaron los reintentos de escritura."

# ============================================================
# GENERADOR DE PDF 58mm
# ============================================================
def generar_pdf_58mm(titulo: str, lineas: list) -> bytes:
    if not REPORTLAB_OK:
        raise RuntimeError("reportlab no está instalado. Agrégalo a requirements.txt.")
    ANCHO_MM   = 58
    MARGEN_MM  = 3
    LINEA_H_MM = 4.2
    FUENTE_NORMAL = 7.5
    FUENTE_BOLD   = 8
    FUENTE_SMALL  = 6.5
    alto_mm  = max(20 + len(lineas) * LINEA_H_MM + 10, 40)
    ancho_pts = ANCHO_MM * mm
    alto_pts  = alto_mm  * mm
    margen_pts = MARGEN_MM * mm
    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=(ancho_pts, alto_pts))
    y = alto_pts - (5 * mm)
    for linea in lineas:
        texto, estilo = linea if isinstance(linea, tuple) else (linea, 'normal')
        if estilo == 'divider':
            c.setFont("Courier", FUENTE_SMALL)
            c.drawString(margen_pts, y, "-" * 30)
        elif estilo == 'bold':
            c.setFont("Courier-Bold", FUENTE_BOLD)
            c.drawString(margen_pts, y, str(texto)[:int((ANCHO_MM - MARGEN_MM*2)/(FUENTE_NORMAL*0.6))])
        elif estilo == 'small':
            c.setFont("Courier", FUENTE_SMALL)
            c.drawString(margen_pts, y, str(texto)[:int((ANCHO_MM - MARGEN_MM*2)/(FUENTE_NORMAL*0.6))])
        elif estilo == 'title':
            c.setFont("Courier-Bold", FUENTE_BOLD + 1)
            c.drawString(margen_pts, y, str(texto)[:int((ANCHO_MM - MARGEN_MM*2)/(FUENTE_NORMAL*0.6))])
        else:
            c.setFont("Courier", FUENTE_NORMAL)
            c.drawString(margen_pts, y, str(texto)[:int((ANCHO_MM - MARGEN_MM*2)/(FUENTE_NORMAL*0.6))])
        y -= LINEA_H_MM * mm
        if y < (5 * mm):
            c.showPage()
            y = alto_pts - (5 * mm)
    c.save()
    buf.seek(0)
    return buf.read()

# ============================================================
# CONEXIÓN A GOOGLE SHEETS
# ============================================================
@st.cache_resource
def conectar_google_sheets():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=scope
        )
        client = gspread.authorize(creds)
        return client.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        st.error(f"Error crítico de conexión con Google Sheets: {e}")
        return None

sh = conectar_google_sheets()

# ============================================================
# MIGRACIÓN DE ESQUEMA
# ============================================================
def _migrar_encabezado_tara():
    if sh is None or st.session_state.get("_tara_migrada", False):
        return
    IDX_TARA_HIS = COLS_HISTORIAL.index("Tara")
    col_his = chr(ord("A") + IDX_TARA_HIS)
    for hoja in ("Historial", "Cierres"):
        ws, err = safe_worksheet(sh, hoja)
        if ws is None:
            continue
        try:
            if "Tara" not in ws.row_values(1):
                ws.update(range_name=f"{col_his}1", values=[["Tara"]])
        except Exception:
            pass
    IDX_TARA_INS = COLS_INSUMOS.index("Tara")
    col_ins = chr(ord("A") + IDX_TARA_INS)
    ws_ins, _ = safe_worksheet(sh, "Insumos")
    if ws_ins is not None:
        try:
            if "Tara" not in ws_ins.row_values(1):
                ws_ins.update(range_name=f"{col_ins}1", values=[["Tara"]])
        except Exception:
            pass

    # ← NUEVO: migrar encabezado "Activo" en columna R de la pestaña Insumos
    IDX_ACTIVO_INS = COLS_INSUMOS.index("Activo")
    col_activo = chr(ord("A") + IDX_ACTIVO_INS)  # = "R"
    if ws_ins is not None:
        try:
            encabezados_actuales = ws_ins.row_values(1)
            if "Activo" not in encabezados_actuales:
                ws_ins.update(range_name=f"{col_activo}1", values=[["Activo"]])
                # ← NUEVO: para filas existentes sin valor en columna R,
                #           establecer TRUE por defecto (no romper insumos ya activos)
                todos_los_valores = ws_ins.get_all_values()
                num_filas = len(todos_los_valores)
                if num_filas > 1:
                    celdas_a_rellenar = []
                    for fila_idx in range(2, num_filas + 1):
                        fila_datos = todos_los_valores[fila_idx - 1]
                        val_activo = fila_datos[IDX_ACTIVO_INS] if len(fila_datos) > IDX_ACTIVO_INS else ""
                        if str(val_activo).strip() == "":
                            celdas_a_rellenar.append([f"{col_activo}{fila_idx}", "TRUE"])
                    for celda_ref, val in celdas_a_rellenar:
                        ws_ins.update(range_name=celda_ref, values=[[val]])
        except Exception:
            pass
    # ← FIN NUEVO

    st.session_state["_tara_migrada"] = True

_migrar_encabezado_tara()

# ============================================================
# CARGA DE DATOS — INVENTARIO
# ============================================================
@st.cache_data(ttl=30)
def cargar_datos_integrales():
    if sh is None:
        return pd.DataFrame(), pd.DataFrame()
    try:
        ws_ins, err_ins = safe_worksheet(sh, "Insumos")
        if err_ins:
            st.warning(err_ins)
            return pd.DataFrame(), pd.DataFrame()
        ws_his, err_his = safe_worksheet(sh, "Historial")
        if err_his:
            st.warning(err_his)
            return pd.DataFrame(), pd.DataFrame()
        val_ins = ws_ins.get_all_values()
        val_his = ws_his.get_all_values()
        ws_cie, _ = safe_worksheet(sh, "Cierres")
        val_cie   = ws_cie.get_all_values() if ws_cie else []

        def _to_df(vals):
            if len(vals) > 1:
                return pd.DataFrame(vals[1:], columns=vals[0])
            return pd.DataFrame(columns=vals[0] if vals else [])

        df_ins = _to_df(val_ins)
        df_his = _to_df(val_his)
        df_cie = _to_df(val_cie) if val_cie else pd.DataFrame()

        df_ins["Sheet_Row_Num"] = df_ins.index + 2
        df_ins = normalizar_dataframe(df_ins, COLS_INSUMOS + ["Sheet_Row_Num"],
                                      cols_criticas=COLS_CRITICAS_INSUMOS)

        # ← NUEVO: filtrar df_ins para que las vistas de Captura e Inventario
        #           solo vean insumos activos (columna R = "Activo" == "TRUE").
        #           Se aplica ÚNICAMENTE sobre df_ins (catálogo).
        #           df_his y df_cie NO se filtran: los datos históricos se preservan íntegros.
        if "Activo" in df_ins.columns:
            df_ins_activos = df_ins[
                df_ins["Activo"].astype(str).str.strip().str.upper() == "TRUE"
            ].copy()
        else:
            # Si la columna aún no existe en el sheet (antes de la migración),
            # tratamos todos los insumos como activos para no romper nada.
            df_ins_activos = df_ins.copy()
        # ← FIN NUEVO

        df_his = normalizar_dataframe(df_his, COLS_HISTORIAL,
                                      cols_criticas=COLS_CRITICAS_HISTORIAL)

        if not df_cie.empty:
            df_cie   = normalizar_dataframe(df_cie, COLS_HISTORIAL)
            df_total = pd.concat([df_cie, df_his], ignore_index=True)
        else:
            df_total = df_his

        if not df_total.empty:
            df_total["Fecha de Inventario"] = pd.to_datetime(df_total["Fecha de Inventario"], errors="coerce")
            df_total["Fecha de Entrada"]    = pd.to_datetime(df_total["Fecha de Entrada"], errors="coerce")

            # ← NUEVO: el merge de metadatos del catálogo usa df_ins_activos para enriquecer
            #           el historial. Sin embargo, filas históricas de insumos desactivados
            #           siguen presentes en df_total (solo quedan sin metadatos actualizados
            #           del catálogo, lo cual es correcto: preservan sus propios valores históricos).
            if not df_ins_activos.empty:
                df_ins_m = df_ins_activos.copy()
            # ← FIN NUEVO
            elif not df_ins.empty:
                df_ins_m = df_ins.copy()
            else:
                df_ins_m = pd.DataFrame()

            if not df_ins_m.empty:
                df_ins_m["_nom_norm"] = df_ins_m["Nombre del Insumo"].apply(normalizar_nombre)
                df_ins_m["_clave"]   = df_ins_m["Unidad de Negocio"].fillna("") + "||" + df_ins_m["_nom_norm"]
                COLS_ESTATICAS = ["_clave","Nombre del Insumo","Marca","Proveedor","Grupo",
                                  "Presentación de Compra","Unidad de Medida","Stock Mínimo","Tara"]
                df_ins_m = df_ins_m[[c for c in COLS_ESTATICAS if c in df_ins_m.columns]].copy()
                df_ins_m["Stock Mínimo"] = df_ins_m["Stock Mínimo"].apply(limpiar_valor)
                df_ins_m["Tara"] = df_ins_m["Tara"].apply(limpiar_valor) if "Tara" in df_ins_m.columns else 0.0
                df_ins_m = df_ins_m.drop_duplicates(subset=["_clave"], keep="last")

                df_total["_nom_norm"] = df_total["Nombre del Insumo"].apply(normalizar_nombre)
                df_total["_clave"]   = df_total["Unidad de Negocio"].fillna("") + "||" + df_total["_nom_norm"]

                COLS_CIFRAS = ["_clave","Unidad de Negocio","Alm","Barra","Stock Neto","¿Comprar?",
                               "Responsable","Fecha de Inventario","Fecha de Entrada","Tara","Observaciones"]
                cols_cifras_ok = [c for c in COLS_CIFRAS if c in df_total.columns]
                df_cifras = df_total[cols_cifras_ok].copy()
                df_total  = df_cifras.merge(df_ins_m, on="_clave", how="left", suffixes=("_hist","_cat"))

                tara_hist = df_total.get("Tara_hist", pd.Series(0.0, index=df_total.index))
                tara_cat  = df_total.get("Tara_cat",  df_total.get("Tara", pd.Series(0.0, index=df_total.index)))
                df_total["Tara"] = tara_hist.apply(limpiar_valor).where(
                    tara_hist.apply(limpiar_valor) > 0, tara_cat.apply(limpiar_valor)
                )
                df_total.drop(columns=["Tara_hist","Tara_cat","_clave","_nom_norm"],
                              inplace=True, errors="ignore")

        # ← NUEVO: devolver df_ins_activos como primer elemento del tuple,
        #           en lugar del df_ins completo (que incluía insumos desactivados).
        #           Todas las páginas que usan df_raw (Inventario, Ingresos, Impresion, Sidebar)
        #           recibirán automáticamente solo insumos activos.
        #           df_total (historial) permanece sin filtrar.
        return df_ins_activos, df_total
        # ← FIN NUEVO

    except Exception as e:
        st.error(f"Falla en extracción de datos: {e}")
        return pd.DataFrame(), pd.DataFrame()

# ============================================================
# CARGA DE DATOS — USUARIOS
# ============================================================
@st.cache_data(ttl=60)
def obtener_usuarios():
    if sh is None:
        return {}, [], pd.DataFrame()
    ws, err = safe_worksheet(sh, "Accesos")
    if err:
        try:
            ws = sh.add_worksheet(title="Accesos", rows="100", cols="3")
            ws.append_row(COLS_ACCESOS)
            ws.append_rows([
                ["13070518","Raúl","admin"],
                ["987654","Jenny","barista"],
                ["ilecara","Araceli","barista"],
            ])
        except Exception as e:
            st.warning(f"No se pudo crear hoja Accesos: {e}")
            return {}, [], pd.DataFrame()
    try:
        data = ws.get_all_values()
        if len(data) < 2:
            return {}, [], pd.DataFrame()
        df_usr = pd.DataFrame(data[1:], columns=data[0])
        for col in COLS_ACCESOS:
            if col not in df_usr.columns:
                df_usr[col] = ""
        usuarios_dict = {
            str(r["Clave"]): {"nombre": str(r["Nombre"]), "rol": str(r["Rol"])}
            for _, r in df_usr.iterrows()
            if str(r.get("Clave","")).strip()
        }
        return usuarios_dict, df_usr["Nombre"].dropna().tolist(), df_usr
    except Exception as e:
        st.warning(f"Error cargando usuarios: {e}")
        return {}, [], pd.DataFrame()

USUARIOS_PIN, LISTA_RESPONSABLES, DF_USUARIOS = obtener_usuarios()

# ============================================================
# CARGA DE DATOS — VENTAS
# ============================================================
@st.cache_data(ttl=30)
def cargar_ventas():
    if sh is None:
        return pd.DataFrame()
    ws, err = safe_worksheet(sh, "Ventas")
    if err:
        return pd.DataFrame()
    try:
        data = ws.get_all_values()
        if len(data) < 2:
            return pd.DataFrame(columns=COLS_VENTAS)
        df = pd.DataFrame(data[1:], columns=data[0])
        for col in COLS_VENTAS:
            if col not in df.columns:
                df[col] = ""
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
        for col in ["Efectivo","Transferencias","Tarjeta","Total_POS","Uber_Eats","Rappi",
                    "Venta_Diaria","Tickets_POS","Tickets_Uber","Tickets_Rappi","Total_Tickets",
                    "Ticket_Promedio","Meta_Mensual","Dias_Habiles","Meta_Diaria"]:
            if col in df.columns:
                df[col] = df[col].apply(limpiar_valor)
        return df
    except Exception as e:
        st.warning(f"Error cargando ventas: {e}")
        return pd.DataFrame()

# ============================================================
# SISTEMA DE AVISOS
# ============================================================
@st.cache_data(ttl=30)
def cargar_avisos():
    if sh is None:
        return pd.DataFrame()
    ws, err = safe_worksheet(sh, "Avisos")
    if err:
        return pd.DataFrame()
    try:
        data = ws.get_all_values()
        if len(data) < 2:
            return pd.DataFrame(columns=COLS_AVISOS)
        df = pd.DataFrame(data[1:], columns=data[0])
        for col in COLS_AVISOS:
            if col not in df.columns:
                df[col] = ""
        return df
    except Exception:
        return pd.DataFrame()

def mostrar_avisos():
    df_av = cargar_avisos()
    if df_av.empty:
        return
    activos = df_av[df_av["Activo"].astype(str).str.upper() == "TRUE"]
    if activos.empty:
        return
    ICONOS = {"info":"ℹ️","warning":"⚠️","urgent":"🚨"}
    FNS    = {"info":st.info,"warning":st.warning,"urgent":st.error}
    for _, av in activos.iterrows():
        tipo = str(av.get("Tipo","info")).lower()
        FNS.get(tipo, st.info)(
            f"{ICONOS.get(tipo,'ℹ️')} **{av.get('Título','')}**  \n{av.get('Mensaje','')}"
        )

# ============================================================
# LÓGICA DE NEGOCIO — INVENTARIO
# ============================================================
def obtener_ultimo_inventario(df_hist: pd.DataFrame, unidad: str = None) -> pd.DataFrame:
    if df_hist.empty:
        return pd.DataFrame()
    df_u = df_hist.copy()
    if unidad:
        df_u = df_u[df_u["Unidad de Negocio"] == unidad]
    if df_u.empty:
        return pd.DataFrame()
    df_u["_fecha_efectiva"] = df_u["Fecha de Inventario"].combine_first(df_u["Fecha de Entrada"])
    df_u["_nombre_norm"]    = df_u["Nombre del Insumo"].apply(normalizar_nombre)
    df_actual = (
        df_u.sort_values("_fecha_efectiva", ascending=True, na_position="first")
            .drop_duplicates(subset=["Unidad de Negocio","_nombre_norm"], keep="last")
            .copy()
    )
    for col in ["Alm","Barra","Stock Neto","Stock Mínimo"]:
        df_actual[col] = df_actual[col].apply(limpiar_valor)
    df_actual["Tara"] = df_actual["Tara"].apply(limpiar_valor) if "Tara" in df_actual.columns else 0.0
    df_actual["Stock Neto Calculado"] = df_actual["Alm"] + df_actual["Barra"]
    if "¿Comprar?" in df_actual.columns:
        df_actual["Necesita Compra"] = df_actual["¿Comprar?"].astype(str).str.strip().str.upper() == "TRUE"
    else:
        df_actual["Necesita Compra"] = df_actual["Stock Neto Calculado"] < df_actual["Stock Mínimo"]
    df_actual["Fecha de Inventario"] = df_actual["_fecha_efectiva"]
    df_actual.drop(columns=["_fecha_efectiva","_nombre_norm"], inplace=True, errors="ignore")
    return df_actual

def fecha_max_segura(serie: pd.Series) -> str:
    validas = serie.dropna()
    if validas.empty:
        return "Sin registros"
    return fmt_fecha_hmo(validas.max())

def buscar_insumo_en_actual(df_actual: pd.DataFrame, nombre: str) -> pd.Series:
    if df_actual.empty:
        return None
    nom_norm = normalizar_nombre(nombre)
    mascaras = df_actual["Nombre del Insumo"].apply(normalizar_nombre) == nom_norm
    if not mascaras.any():
        return None
    return df_actual[mascaras].iloc[0]

def construir_fila_historial(
    unidad, nombre, marca, proveedor, grupo, fecha_entrada,
    presentacion, unidad_medida, alm, barra, stock_neto,
    stock_minimo, comprar, responsable, fecha_inventario, tara, observaciones,
) -> list:
    def _s(v):
        if v is None: return ""
        try:
            if pd.isna(v): return ""
        except Exception: pass
        return str(v).strip()
    def _n(v): return limpiar_valor(v)
    return [
        _s(unidad), _s(nombre), _s(marca), _s(proveedor), _s(grupo),
        _s(fecha_entrada), _s(presentacion), _s(unidad_medida),
        _n(alm), _n(barra), _n(stock_neto), _n(stock_minimo),
        "TRUE" if comprar else "FALSE",
        _s(responsable), _s(fecha_inventario),
        max(0.0, _n(tara)), _s(observaciones),
    ]

# ============================================================
# LÓGICA DE NEGOCIO — VENTAS
# ============================================================
def _asegurar_hoja_ventas():
    ws, err = safe_worksheet(sh, "Ventas")
    if err:
        try:
            ws = sh.add_worksheet(title="Ventas", rows="2000", cols=str(len(COLS_VENTAS)))
            ws.append_row(COLS_VENTAS)
            return ws, None
        except Exception as e:
            return None, f"No se pudo crear hoja Ventas: {e}"
    return ws, None

def _construir_fila_venta(
    fecha, efectivo, transferencias, tarjeta, uber, rappi,
    tickets_pos, tickets_uber, tickets_rappi,
    meta_mensual, dias_habiles, responsable, notas,
):
    total_pos    = efectivo + transferencias + tarjeta
    venta_diaria = total_pos + uber + rappi
    total_tix    = tickets_pos + tickets_uber + tickets_rappi
    tix_prom     = round(venta_diaria / total_tix, 2) if total_tix > 0 else 0.0
    meta_diaria  = round(meta_mensual / dias_habiles, 2) if dias_habiles > 0 else 0.0
    return [
        "Noble",
        fecha.strftime("%Y-%m-%d"),
        fecha.day, fecha.month, fecha.year,
        efectivo, transferencias, tarjeta, total_pos,
        uber, rappi, venta_diaria,
        tickets_pos, tickets_uber, tickets_rappi, total_tix,
        tix_prom, meta_mensual, dias_habiles, meta_diaria,
        responsable, notas,
    ]

# ============================================================
# ESTADO DE SESIÓN
# ============================================================
_defaults = {
    "auth_status": False,
    "current_user": None,
    "user_role": None,
    "pagina": "Dashboard",
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

if "responsables" not in st.session_state:
    st.session_state.responsables = LISTA_RESPONSABLES if LISTA_RESPONSABLES else ["Raúl"]

def cambiar_pagina(nombre: str):
    st.session_state.pagina = nombre
    st.rerun()

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    if not st.session_state.auth_status:
        st.subheader("🔒 Identificación")
        st.write("Inicia sesión para editar datos.")
        with st.form("login_form"):
            pin_input = st.text_input("Ingresa tu Clave:", type="password")
            submitted = st.form_submit_button("Desbloquear Sistema", type="primary", use_container_width=True)
            if submitted:
                if pin_input in USUARIOS_PIN:
                    st.session_state.auth_status  = True
                    st.session_state.current_user = USUARIOS_PIN[pin_input]["nombre"]
                    st.session_state.user_role    = USUARIOS_PIN[pin_input]["rol"]
                    st.rerun()
                else:
                    st.error("⚠️ Clave incorrecta o no registrada.")
    else:
        st.write(f"👤 Operador: **{st.session_state.current_user}**")
        if st.button("🚪 Cerrar Sesión", use_container_width=True):
            for k in ["auth_status","current_user","user_role"]:
                st.session_state[k] = _defaults[k]
            st.session_state.pagina = "Dashboard"
            st.rerun()

    st.divider()
    st.title("⚙️ Operaciones Noble")
    if st.button("📊 Dashboard Principal", use_container_width=True):
        cambiar_pagina("Dashboard")

    st.divider()
    st.write("**📦 Movimientos de Stock:**")
    if st.button("📝 1. Capturar inventario",  use_container_width=True): cambiar_pagina("Inventario")
    if st.button("📥 2. Entrada de compras",   use_container_width=True): cambiar_pagina("Ingresos")
    if st.button("📦 3. Inventario actual",    use_container_width=True): cambiar_pagina("Consulta")

    st.divider()
    st.write("**💰 Ventas:**")
    if st.button("📈 Registrar Venta Diaria",  use_container_width=True): cambiar_pagina("Ventas")
    if st.button("📊 Dashboard de Ventas",     use_container_width=True): cambiar_pagina("DashboardVentas")
    if st.button("📥 Importar Histórico",      use_container_width=True): cambiar_pagina("ImportarVentas")

    st.divider()
    st.write("**🖨️ Tickets (58mm):**")
    if st.button("📋 1. Lista de Conteo",      use_container_width=True): cambiar_pagina("Impresion")
    if st.button("🛒 2. Lista de Compra",      use_container_width=True): cambiar_pagina("ListaCompra")
    if st.button("📦 3. Reporte de Stock",     use_container_width=True): cambiar_pagina("ReporteStock")

    st.divider()
    with st.expander("ℹ️ Guía de Clasificación (Grupos)"):
        st.markdown("""
**🔴 Rutina Diaria — Perecederos y Alta Rotación**

**Grupo A — Café, Leches y Lácteos**
Insumos de uso constante en cada turno. Caducan o se agotan rápido. Conteo obligatorio todos los días antes de abrir.
Ejemplos: café en grano, café molido, leche entera, leche de avena, leche de almendra, crema para batir, mantequilla.

**Grupo B — Jarabes, Salsas y Bases Líquidas**
Productos abiertos que se contaminan con el tiempo. Revisar nivel y estado del envase diariamente.
Ejemplos: jarabes Monin/Torani, salsa de caramelo, salsa de chocolate, base de matcha líquida, concentrados de fruta.

**Grupo C — Polvos, Tés y Tisanas**
Sensibles a humedad. Contar en bolsa/bote cerrado. Incluye todo lo que se pesa o dosifica en scoop.
Ejemplos: matcha en polvo, chocolate en polvo, canela, cúrcuma, chai spice, tés de caja, tisanas sueltas.

---

**🟡 Rutina cada 2 Días — Secos y Suministros**

**Grupo D — Empaques y Desechables**
Conteo por pieza o rollo. Incluye todo lo que sale de la tienda con el producto.
Ejemplos: vasos 8/12/16/20 oz, tapas, popotes, servilletas, bolsas de papel, etiquetas térmicas, mangas de cartón.

**Grupo E — Suministros de Limpieza**
Incluye lo que se usa en el área de barra y en el área de preparación. Conteo en mililitros o piezas según presentación.
Ejemplos: desengrasante, cloro, gel antibacterial, franelas, esponjas, cepillos portafiltro, pastillas de limpieza.

**Grupo F — Comida y Vitrina**
Productos para venta directa o preparación de alimentos. Revisión de fecha de caducidad en cada conteo.
Ejemplos: pan para sándwich, pan dulce, muffins, galletas empacadas, snacks, fruta para decoración.

**Grupo G — Retail, Utensilios y Otros**
Todo lo que se vende como producto terminado o se usa como equipo de apoyo. Conteo menos frecuente pero registrar cualquier salida.
Ejemplos: café empacado para venta, merch Noble, filtros de papel, tampers, termómetros de barra, cucharas medidoras.
        """)

    with st.expander("📊 Tabla Resumen de Grupos"):
        grupos_info = [
            {"Grupo":"A","Nombre":"Café, Leches y Lácteos","Rutina":"Diaria","Riesgo":"Alto","Almacén":"Refrigerador / Bodega seca","Nota":"Contar antes de abrir"},
            {"Grupo":"B","Nombre":"Jarabes, Salsas y Bases","Rutina":"Diaria","Riesgo":"Alto","Almacén":"Repisa barra / Refrigerador","Nota":"Revisar envases abiertos"},
            {"Grupo":"C","Nombre":"Polvos, Tés y Tisanas","Rutina":"Diaria","Riesgo":"Medio","Almacén":"Bodega seca hermética","Nota":"Proteger de humedad"},
            {"Grupo":"D","Nombre":"Empaques y Desechables","Rutina":"Cada 2 días","Riesgo":"Medio","Almacén":"Bodega empaques","Nota":"Contar en piezas/rollos"},
            {"Grupo":"E","Nombre":"Suministros de Limpieza","Rutina":"Cada 2 días","Riesgo":"Bajo","Almacén":"Bodega limpieza","Nota":"Separar de alimentos"},
            {"Grupo":"F","Nombre":"Comida y Vitrina","Rutina":"Cada 2 días","Riesgo":"Alto","Almacén":"Vitrina / Refrigerador","Nota":"Verificar caducidad"},
            {"Grupo":"G","Nombre":"Retail, Utensilios y Otros","Rutina":"Cada 2 días","Riesgo":"Bajo","Almacén":"Bodega general / Mostrador","Nota":"Registrar cada salida"},
        ]
        st.dataframe(pd.DataFrame(grupos_info), hide_index=True, use_container_width=True)

    # ZONA ADMIN
    if st.session_state.user_role == "admin":
        st.divider()
        st.write("**🛠️ Administración Avanzada:**")
        if st.button("🔒 Corte de Mes", use_container_width=True):
            cambiar_pagina("CorteMes")

        st.divider()
        with st.expander("👤 Gestión de Accesos"):
            st.write("**Agregar / Actualizar Barista**")
            n_nombre = st.text_input("Nombre de Usuario:")
            n_clave  = st.text_input("Clave de Acceso:")
            n_rol    = st.selectbox("Nivel de Permisos:", ["barista","admin"])
            if st.button("➕ Guardar Usuario", use_container_width=True):
                if n_nombre and n_clave:
                    ws_acc, err = safe_worksheet(sh, "Accesos")
                    if err:
                        st.error(err)
                    else:
                        try:
                            nuevo_df  = DF_USUARIOS.copy()
                            nuevo_df  = nuevo_df[nuevo_df["Nombre"] != n_nombre]
                            nueva_fil = pd.DataFrame([{"Clave":str(n_clave),"Nombre":n_nombre,"Rol":n_rol}])
                            nuevo_df  = pd.concat([nuevo_df, nueva_fil], ignore_index=True)
                            ws_acc.clear()
                            ws_acc.append_row(COLS_ACCESOS)
                            ws_acc.append_rows(nuevo_df[COLS_ACCESOS].values.tolist())
                            st.cache_data.clear()
                            st.success(f"Permisos para '{n_nombre}' guardados.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al guardar: {e}")
                else:
                    st.warning("Completa nombre y clave.")
            st.divider()
            st.write("**Eliminar Barista**")
            if LISTA_RESPONSABLES:
                u_del = st.selectbox("Seleccionar:", LISTA_RESPONSABLES)
                if st.button("❌ Borrar Acceso", use_container_width=True):
                    ws_acc, err = safe_worksheet(sh, "Accesos")
                    if err:
                        st.error(err)
                    else:
                        try:
                            nuevo_df = DF_USUARIOS[DF_USUARIOS["Nombre"] != u_del]
                            ws_acc.clear()
                            ws_acc.append_row(COLS_ACCESOS)
                            ws_acc.append_rows(nuevo_df[COLS_ACCESOS].values.tolist())
                            st.cache_data.clear()
                            st.success(f"Acceso revocado para {u_del}.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
            else:
                st.info("No hay responsables registrados.")

        st.divider()
        with st.expander("📢 Gestión de Avisos"):
            df_av_mgr = cargar_avisos()
            st.write("**Nuevo aviso**")
            with st.form("f_aviso", clear_on_submit=True):
                av_titulo = st.text_input("Título")
                av_msg    = st.text_area("Mensaje", height=80)
                av_tipo   = st.selectbox("Tipo", ["info","warning","urgent"],
                                         format_func=lambda x: {"info":"ℹ️ Informativo","warning":"⚠️ Advertencia","urgent":"🚨 Urgente"}[x])
                if st.form_submit_button("📢 Publicar aviso", use_container_width=True):
                    if not av_titulo.strip() or not av_msg.strip():
                        st.error("Título y mensaje son obligatorios.")
                    else:
                        ws_av, err = safe_worksheet(sh, "Avisos")
                        if err:
                            try:
                                ws_av = sh.add_worksheet(title="Avisos", rows="200", cols="7")
                                ws_av.append_row(COLS_AVISOS)
                            except Exception as e:
                                st.error(f"No se pudo crear hoja Avisos: {e}")
                                ws_av = None
                        if ws_av:
                            import uuid
                            ws_av.append_row([
                                str(uuid.uuid4())[:8], av_titulo.strip(), av_msg.strip(),
                                av_tipo, "TRUE", ts_hermosillo(), st.session_state.current_user,
                            ], value_input_option="USER_ENTERED")
                            st.cache_data.clear()
                            st.success("Aviso publicado.")
                            time.sleep(0.5)
                            st.rerun()
            if not df_av_mgr.empty:
                st.divider()
                st.write("**Avisos existentes**")
                for _, av in df_av_mgr.iterrows():
                    activo = str(av.get("Activo","")).upper() == "TRUE"
                    tipo   = str(av.get("Tipo","info"))
                    icono  = {"info":"ℹ️","warning":"⚠️","urgent":"🚨"}.get(tipo,"ℹ️")
                    estado = "🟢" if activo else "⚫"
                    col_a, col_b = st.columns([3,1])
                    with col_a:
                        st.write(f"{estado} {icono} **{av.get('Título','')}**")
                        st.caption(str(av.get("Mensaje",""))[:80])
                    with col_b:
                        av_id = str(av.get("ID",""))
                        if st.button("Desactivar" if activo else "Activar", key=f"tog_{av_id}", use_container_width=True):
                            ws_av, err = safe_worksheet(sh, "Avisos")
                            if not err:
                                try:
                                    celdas = ws_av.get_all_values()
                                    for i, fila in enumerate(celdas[1:], start=2):
                                        if fila[0] == av_id:
                                            ws_av.update(range_name=f"E{i}", values=[["FALSE" if activo else "TRUE"]])
                                            st.cache_data.clear()
                                            st.rerun()
                                            break
                                except Exception as e:
                                    st.error(f"Error: {e}")

    # CATÁLOGO
    if st.session_state.auth_status:
        # ← NUEVO: cargar df_raw_sb usando el df_ins completo (no filtrado) para que el admin
        #           pueda editar/activar insumos desactivados también.
        #           Se hace una carga directa del sheet sin filtro de Activo para la gestión del catálogo.
        df_raw_sb_full = pd.DataFrame()
        if sh is not None:
            try:
                ws_ins_sb, _ = safe_worksheet(sh, "Insumos")
                if ws_ins_sb is not None:
                    val_ins_sb = ws_ins_sb.get_all_values()
                    if len(val_ins_sb) > 1:
                        df_raw_sb_full = pd.DataFrame(val_ins_sb[1:], columns=val_ins_sb[0])
                        df_raw_sb_full["Sheet_Row_Num"] = df_raw_sb_full.index + 2
                        df_raw_sb_full = normalizar_dataframe(
                            df_raw_sb_full, COLS_INSUMOS + ["Sheet_Row_Num"],
                            cols_criticas=COLS_CRITICAS_INSUMOS
                        )
            except Exception:
                pass
        df_raw_sb = df_raw_sb_full  # el sidebar de catálogo ve todos los insumos (activos e inactivos)
        # ← FIN NUEVO

        st.divider()
        st.subheader("🛠️ Gestión del Catálogo")
        op_cat = st.radio("Acción:", ["Añadir Insumo","Editar Insumo"])

        if op_cat == "Añadir Insumo":
            with st.form("f_add", clear_on_submit=True):
                u  = st.selectbox("Unidad", UNIDADES)
                n  = st.text_input("Nombre del Insumo")
                m  = st.text_input("Marca")
                p  = st.text_input("Proveedor")
                g  = st.selectbox("Grupo", GRUPOS)
                uc = st.text_input("Presentación de Compra")
                um = st.selectbox("Unidad de Medida", UNIDADES_MED)
                sm = st.number_input("Stock Mínimo", min_value=0.0)
                tara_new = st.number_input("Tara (kg/gr)", min_value=0.0, value=0.0)
                # ← NUEVO: todo insumo creado desde el formulario nace como Activo = TRUE
                if st.form_submit_button("✨ Crear Insumo"):
                    if not n.strip():
                        st.error("El nombre del insumo es obligatorio.")
                    else:
                        ws_ins, err = safe_worksheet(sh, "Insumos")
                        if err:
                            st.error(err)
                        else:
                            try:
                                ws_ins.append_row(
                                    [u, n.strip(), m, p, g, "", uc, um, "", "", "", sm, "", "", "", "", tara_new, "TRUE"],  # ← NUEVO: "TRUE" en columna R
                                    value_input_option="USER_ENTERED"
                                )
                                st.cache_data.clear()
                                st.success(f"Insumo '{n.strip()}' creado.")
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error al crear insumo: {e}")

        else:  # Editar Insumo
            if df_raw_sb.empty or "Nombre del Insumo" not in df_raw_sb.columns:
                st.info("Sin insumos disponibles para editar.")
            else:
                ins_nombres = df_raw_sb["Nombre del Insumo"].dropna().unique().tolist()
                if not ins_nombres:
                    st.info("El catálogo está vacío.")
                else:
                    # ← NUEVO: mostrar indicador visual de estado (activo/inactivo) en el selector
                    def _label_insumo(nombre):
                        mask = df_raw_sb["Nombre del Insumo"] == nombre
                        if not mask.any():
                            return nombre
                        val_activo = str(df_raw_sb[mask].iloc[0].get("Activo", "TRUE")).strip().upper()
                        return nombre if val_activo == "TRUE" else f"⛔ {nombre} (inactivo)"

                    ins_edit = st.selectbox(
                        "Seleccionar Insumo a Editar:",
                        sorted(ins_nombres),
                        format_func=_label_insumo
                    )
                    # ← FIN NUEVO
                    mask = df_raw_sb["Nombre del Insumo"] == ins_edit
                    if not mask.any():
                        st.warning("Insumo no encontrado.")
                    else:
                        d = df_raw_sb[mask].iloc[0]
                        with st.form("f_edit"):
                            unidad_val = d.get("Unidad de Negocio", UNIDADES[0])
                            e_u  = st.selectbox("Unidad", UNIDADES, index=UNIDADES.index(unidad_val) if unidad_val in UNIDADES else 0)
                            e_n  = st.text_input("Nombre", value=str(d.get("Nombre del Insumo","")))
                            e_m  = st.text_input("Marca", value=str(d.get("Marca","")))
                            e_p  = st.text_input("Proveedor", value=str(d.get("Proveedor","")))
                            grupo_val = str(d.get("Grupo","A"))
                            e_g  = st.selectbox("Grupo", GRUPOS, index=GRUPOS.index(grupo_val) if grupo_val in GRUPOS else 0)
                            e_uc = st.text_input("Presentación Compra", value=str(d.get("Presentación de Compra","")))
                            u_val = str(d.get("Unidad de Medida","pz")).lower()
                            e_um = st.selectbox("Medida", UNIDADES_MED, index=UNIDADES_MED.index(u_val) if u_val in UNIDADES_MED else 0)
                            e_sm = st.number_input("Stock Mínimo", min_value=0.0, value=limpiar_valor(d.get("Stock Mínimo",0)))
                            e_tara = st.number_input("Tara (kg/gr)", min_value=0.0, value=limpiar_valor(d.get("Tara",0)))
                            # ← NUEVO: toggle Activo/Inactivo en el formulario de edición
                            activo_actual = str(d.get("Activo", "TRUE")).strip().upper() == "TRUE"
                            e_activo = st.toggle(
                                "Insumo Activo",
                                value=activo_actual,
                                help="Desactiva para ocultarlo de Captura e Inventario sin borrar su historial."
                            )
                            # ← FIN NUEVO
                            if st.form_submit_button("💾 Actualizar Insumo"):
                                if not e_n.strip():
                                    st.error("El nombre no puede quedar vacío.")
                                else:
                                    ws_ins, err = safe_worksheet(sh, "Insumos")
                                    if err:
                                        st.error(err)
                                    else:
                                        try:
                                            idx = int(d.get("Sheet_Row_Num",0))
                                            if idx < 2:
                                                raise ValueError("Número de fila inválido.")
                                            ws_ins.update(
                                                range_name=f"A{idx}:R{idx}",  # ← NUEVO: extendido de Q a R para incluir Activo
                                                values=[[e_u, e_n.strip(), e_m, e_p, e_g, "", e_uc, e_um, "", "", "", e_sm, "", "", "", "", e_tara, "TRUE" if e_activo else "FALSE"]]  # ← NUEVO: columna R
                                            )
                                            st.cache_data.clear()
                                            st.success("Catálogo actualizado.")
                                            time.sleep(1)
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Error al actualizar: {e}")

# ============================================================
# PÁGINAS
# ============================================================
pagina = st.session_state.pagina

# ── DASHBOARD ────────────────────────────────────────────────
if pagina == "Dashboard":
    df_raw, df_historial = cargar_datos_integrales()
    st.title("📊 Dashboard Operativo")

    ahora = ahora_hermosillo()
    dias_faltantes = calendar.monthrange(ahora.year, ahora.month)[1] - ahora.day
    if dias_faltantes <= 4:
        st.info(f"⏳ A {dias_faltantes} días del fin de mes. Recuerda ejecutar el **Corte de Mes**.")

    df_actual = obtener_ultimo_inventario(df_historial)

    if not df_actual.empty:
        crit = df_actual[df_actual["Necesita Compra"] == True]
        c1, c2, c3 = st.columns(3)
        c1.metric("🛒 Pendientes Noble",          len(crit[crit["Unidad de Negocio"] == "Noble"]))
        c2.metric("🛒 Pendientes Coffee Station",  len(crit[crit["Unidad de Negocio"] == "Coffee Station"]))
        c3.metric("🕒 Último Movimiento",          fecha_max_segura(df_actual["Fecha de Inventario"]))

        st.divider()

        def tabla_pendientes(df_pen: pd.DataFrame, titulo: str):
            if df_pen.empty:
                st.success(f"✅ {titulo} — Operación cubierta.")
                return
            st.markdown(f"#### {titulo} — {len(df_pen)} pendiente(s)")
            df_pen = df_pen.copy()
            df_pen["_brecha"] = df_pen["Stock Neto Calculado"] - df_pen["Stock Mínimo"]
            df_pen = df_pen.sort_values(["Grupo","_brecha"], ascending=[True, True])
            h = st.columns([0.5, 2.5, 1.0, 1.0, 1.0, 1.8])
            for col, label in zip(h, ["Grp","Insumo","Stock","Mín","Brecha","Último conteo"]):
                col.markdown(f"<small><b>{label}</b></small>", unsafe_allow_html=True)
            grupo_anterior = None
            for _, r in df_pen.iterrows():
                grupo  = str(r.get("Grupo","—"))
                stock  = r["Stock Neto Calculado"]
                minimo = r["Stock Mínimo"]
                brecha = stock - minimo
                fecha_str = fmt_fecha_hmo(r.get("Fecha de Inventario"))
                if grupo != grupo_anterior:
                    st.markdown(
                        f"<div style='margin:6px 0 2px;font-size:11px;color:var(--color-text-tertiary);letter-spacing:.08em'>GRUPO {grupo}</div>",
                        unsafe_allow_html=True
                    )
                    grupo_anterior = grupo
                color_dot = "#E24B4A" if brecha < 0 else ("#EF9F27" if brecha < minimo * 0.3 else "#639922")
                color_txt = "#E24B4A" if brecha < 0 else ("#BA7517" if brecha < minimo * 0.3 else "#3B6D11")
                pct = min(100, int((stock / minimo * 100) if minimo > 0 else 100))
                barra_html = (
                    f"<div style='background:var(--color-border-tertiary);border-radius:4px;height:6px;width:100%;margin-top:4px'>"
                    f"<div style='width:{pct}%;height:6px;border-radius:4px;background:{color_dot}'></div></div>"
                )
                c = st.columns([0.5, 2.5, 1.0, 1.0, 1.0, 1.8])
                c[0].markdown(f"<span style='font-size:16px;line-height:2;color:{color_dot}'>●</span>", unsafe_allow_html=True)
                c[1].markdown(f"<span style='font-size:13px;font-weight:500'>{r['Nombre del Insumo']}</span>", unsafe_allow_html=True)
                c[2].markdown(f"<span style='color:{color_txt};font-weight:500'>{stock:.1f}</span>", unsafe_allow_html=True)
                c[3].markdown(f"<span style='font-size:13px'>{minimo:.1f}</span>", unsafe_allow_html=True)
                c[4].markdown(f"<span style='color:{color_txt};font-weight:500'>{brecha:+.1f}</span>{barra_html}", unsafe_allow_html=True)
                c[5].markdown(f"<span style='font-size:11px;color:var(--color-text-secondary)'>{fecha_str}</span>", unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🏢 Noble")
            tabla_pendientes(crit[crit["Unidad de Negocio"] == "Noble"], "Noble")
        with col2:
            st.subheader("☕ Coffee Station")
            tabla_pendientes(crit[crit["Unidad de Negocio"] == "Coffee Station"], "Coffee Station")

        st.divider()
        st.subheader("🕒 Actividad Reciente")
        df_log = df_historial.copy()
        df_log["Fecha de Inventario"] = df_log["Fecha de Inventario"].combine_first(df_log["Fecha de Entrada"])
        cols_log = ["Fecha de Inventario","Responsable","Unidad de Negocio","Nombre del Insumo","Stock Neto","¿Comprar?","Observaciones"]
        cols_log_ok = [c for c in cols_log if c in df_log.columns]
        st.dataframe(
            df_log.dropna(subset=["Fecha de Inventario"])
                  .sort_values("Fecha de Inventario", ascending=False)[cols_log_ok]
                  .head(15),
            use_container_width=True
        )
    else:
        st.info("Sin datos históricos. Ejecuta el primer conteo de inventario.")

# ── INVENTARIO ───────────────────────────────────────────────
elif pagina == "Inventario":
    df_raw, df_historial = cargar_datos_integrales()
    st.title("📝 Capturar inventario")
    mostrar_avisos()
    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida.")
        st.stop()

    col_u, col_r, col_g = st.columns([1,1,2])
    with col_u:
        u_sel = st.selectbox("🏢 Unidad de Negocio", UNIDADES)
    responsables = st.session_state.responsables or ["Raúl"]
    resp_idx = responsables.index(st.session_state.current_user) if st.session_state.current_user in responsables else 0
    with col_r:
        r_sel = st.selectbox("👤 Responsable", responsables, index=resp_idx,
                             disabled=(st.session_state.user_role != "admin"))
    # ← NUEVO: df_raw ya viene filtrado (solo activos), no se requiere cambio aquí
    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    with col_g:
        grps  = sorted(df_u["Grupo"].dropna().unique().tolist()) if not df_u.empty and "Grupo" in df_u.columns else GRUPOS
        g_sel = st.multiselect("📂 Grupos a contar", grps, default=grps[:1] if grps else [])

    st.divider()
    busqueda_inv = st.text_input("🔍 Buscar insumo:", placeholder="Escribe el nombre...")
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    df_f = (
        df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo","Nombre del Insumo"]).reset_index(drop=True)
        if not df_u.empty and g_sel else pd.DataFrame()
    )
    if busqueda_inv and not df_f.empty:
        df_f = df_f[df_f["Nombre del Insumo"].astype(str).str.contains(busqueda_inv, case=False, na=False)]

    if df_f.empty:
        st.info("Selecciona al menos un grupo para mostrar insumos.")
    else:
        with st.form("form_inventario", clear_on_submit=False):
            h1,h2,h3,h4,h5,h6,h7,h8 = st.columns([2.8,1.0,1.0,1.0,1.0,1.0,1.2,2.5])
            for col, label in zip([h1,h2,h3,h4,h5,h6,h7,h8],
                                  ["Insumo / Ref","Almacén","Barra (bruto)","Medida","Tara (gr)","Neto*","¿Pedir?","Observaciones"]):
                col.write(f"**{label}**")
            st.markdown("*Neto = Alm + (Barra − Tara). La Tara se descuenta solo de Barra.")
            st.divider()

            regs_form = {}
            for idx_row, row in df_f.iterrows():
                nom      = str(row.get("Nombre del Insumo",""))
                safe_nom = re.sub(r'[^a-zA-Z0-9]','_', nom)[:35] + f"_{idx_row}"
                prev        = buscar_insumo_en_actual(df_actual, nom)
                v_prev      = prev["Stock Neto Calculado"] if prev is not None else 0.0
                v_alm_prev  = limpiar_valor(prev["Alm"])   if prev is not None else 0.0
                v_bar_prev  = limpiar_valor(prev["Barra"]) if prev is not None else 0.0
                v_min       = limpiar_valor(row.get("Stock Mínimo",0))
                v_tara_hist = limpiar_valor(prev.get("Tara",0)) if prev is not None else 0.0
                v_tara_cat  = limpiar_valor(row.get("Tara",0))
                v_tara_init = v_tara_hist if v_tara_hist > 0 else v_tara_cat

                c1,c2,c3,c4,c5,c6,c7,c8 = st.columns([2.8,1.0,1.0,1.0,1.0,1.0,1.2,2.5])
                with c1:
                    st.write(f"**{nom}**")
                    st.caption(f"Marca: {row.get('Marca','-')} | Prov: {row.get('Proveedor','-')}")
                    diff  = v_prev - v_min
                    color = "green" if diff >= 0 else "red"
                    tara_txt = f" | Tara: {v_tara_init}" if v_tara_init > 0 else ""
                    st.markdown(
                        f"<small>Anterior: {v_prev} | Mín: {v_min} (<span style='color:{color}'>{diff:+.1f}</span>){tara_txt}</small>",
                        unsafe_allow_html=True
                    )
                with c2:
                    alm_key = f"a_{safe_nom}"
                    if alm_key not in st.session_state: st.session_state[alm_key] = v_alm_prev
                    v_a = st.number_input("Alm", min_value=0.0, step=1.0, key=alm_key, label_visibility="collapsed")
                with c3:
                    bar_key = f"b_{safe_nom}"
                    if bar_key not in st.session_state: st.session_state[bar_key] = v_bar_prev
                    v_b = st.number_input("Bar", min_value=0.0, step=1.0, key=bar_key, label_visibility="collapsed")
                with c4:
                    u_act = str(row.get("Unidad de Medida","pz")).lower()
                    v_u = st.selectbox("U", UNIDADES_MED,
                                       index=UNIDADES_MED.index(u_act) if u_act in UNIDADES_MED else 0,
                                       key=f"u_{safe_nom}", label_visibility="collapsed")
                with c5:
                    tara_key = f"tara_{safe_nom}"
                    if tara_key not in st.session_state: st.session_state[tara_key] = v_tara_init
                    v_tara_manual = st.number_input("Tara", min_value=0.0, step=0.1,
                                                    key=tara_key, label_visibility="collapsed")
                with c6:
                    v_b_neto    = max(0.0, v_b - v_tara_manual)
                    v_n_display = v_a + v_b_neto
                    st.write(f"**{v_n_display:.1f}**")
                with c7:
                    v_comprar_prev = bool(prev.get("Necesita Compra",False)) if prev is not None else False
                    ck_key = f"p_{safe_nom}"
                    if ck_key not in st.session_state: st.session_state[ck_key] = v_comprar_prev
                    v_p = st.checkbox("🛒", key=ck_key)
                with c8:
                    v_c = st.text_input("Obs", key=f"c_{safe_nom}", label_visibility="collapsed", placeholder="Opcional")

                regs_form[nom] = {"a":v_a,"b":v_b_neto,"n":v_n_display,"u":v_u,"p":v_p,"c":v_c,"tara":v_tara_manual,"row":row}
                st.divider()

            btn_inv = st.form_submit_button("📥 PROCESAR INVENTARIO", use_container_width=True, type="primary")

        if btn_inv:
            ws_his, err = safe_worksheet(sh, "Historial")
            if err:
                st.error(err)
            else:
                fh    = ts_hermosillo()
                filas = []
                for n, info in regs_form.items():
                    dm = info["row"]
                    filas.append(construir_fila_historial(
                        unidad=u_sel, nombre=n, marca=dm.get("Marca",""),
                        proveedor=dm.get("Proveedor",""), grupo=dm.get("Grupo",""),
                        fecha_entrada="", presentacion=dm.get("Presentación de Compra",""),
                        unidad_medida=info["u"], alm=info["a"], barra=info["b"],
                        stock_neto=info["n"], stock_minimo=dm.get("Stock Mínimo",0),
                        comprar=info["p"], responsable=r_sel, fecha_inventario=fh,
                        tara=info["tara"], observaciones=info["c"],
                    ))
                ok, msg = append_rows_con_retry(ws_his, filas)
                if ok:
                    st.cache_data.clear()
                    st.success(f"¡Inventario registrado! {msg}")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error(msg)

# ── INGRESOS ─────────────────────────────────────────────────
elif pagina == "Ingresos":
    df_raw, df_historial = cargar_datos_integrales()
    st.title("📥 Entrada de compras")
    mostrar_avisos()
    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida.")
        st.stop()
    st.info("Ingresa insumos recibidos. Se sumarán al último stock de Almacén registrado.")
    col_u, col_r = st.columns(2)
    with col_u:
        u_sel = st.selectbox("🏢 Unidad receptora:", UNIDADES)
    responsables = st.session_state.responsables or ["Raúl"]
    resp_idx = responsables.index(st.session_state.current_user) if st.session_state.current_user in responsables else 0
    with col_r:
        r_sel = st.selectbox("👤 Responsable:", responsables, index=resp_idx,
                             disabled=(st.session_state.user_role != "admin"))
    # ← NUEVO: df_raw ya viene filtrado (solo activos), no se requiere cambio aquí
    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    if df_u.empty:
        st.warning("Sin insumos registrados para esta unidad.")
        st.stop()
    df_actual    = obtener_ultimo_inventario(df_historial, u_sel)
    nombres_ins  = df_u["Nombre del Insumo"].dropna().unique().tolist()
    st.divider()
    modo_bulk = st.toggle("🚀 Activar Ingreso Masivo Rápido (Bulk)")

    if modo_bulk:
        st.subheader("Carga Bulk")
        bulk_data = []
        for _, r in df_u.iterrows():
            nom = r["Nombre del Insumo"]
            prev = buscar_insumo_en_actual(df_actual, nom)
            bulk_data.append({"Insumo":nom, "Stock Alm":prev["Alm"] if prev is not None else 0.0,
                               "Stock Barra":prev["Barra"] if prev is not None else 0.0, "+ Ingreso":0.0})
        df_edit   = pd.DataFrame(bulk_data)
        edited_df = st.data_editor(df_edit[["Insumo","Stock Alm","Stock Barra","+ Ingreso"]],
                                   hide_index=True, use_container_width=True,
                                   disabled=["Insumo","Stock Alm","Stock Barra"])
        proc_bulk = st.session_state.get("_procesando_bulk", False)
        btn_bulk  = st.button("📦 EJECUTAR INGRESO BULK", type="primary", disabled=proc_bulk)
        if btn_bulk and not proc_bulk:
            st.session_state["_procesando_bulk"] = True
            ws_his, err = safe_worksheet(sh, "Historial")
            if err:
                st.error(err)
                st.session_state["_procesando_bulk"] = False
            else:
                fh = ts_hermosillo()
                filas_bulk = []
                for _, r_ed in edited_df.iterrows():
                    ingreso = limpiar_valor(r_ed["+ Ingreso"])
                    if ingreso <= 0: continue
                    nom  = r_ed["Insumo"]
                    orig = next((x for x in bulk_data if x["Insumo"] == nom), None)
                    if orig is None: continue
                    row_matches = df_u[df_u["Nombre del Insumo"] == nom]
                    if row_matches.empty: continue
                    row_ins = row_matches.iloc[0]
                    v_min   = limpiar_valor(row_ins.get("Stock Mínimo",0))
                    tara_bulk = limpiar_valor(row_ins.get("Tara",0))
                    nuevo_a   = orig["Stock Alm"] + ingreso
                    nuevo_n   = nuevo_a + orig["Stock Barra"]
                    filas_bulk.append(construir_fila_historial(
                        unidad=u_sel, nombre=nom, marca=row_ins.get("Marca",""),
                        proveedor=row_ins.get("Proveedor",""), grupo=row_ins.get("Grupo",""),
                        fecha_entrada=fh, presentacion=row_ins.get("Presentación de Compra",""),
                        unidad_medida=row_ins.get("Unidad de Medida","pz"), alm=nuevo_a,
                        barra=orig["Stock Barra"], stock_neto=nuevo_n, stock_minimo=v_min,
                        comprar=nuevo_n < v_min, responsable=r_sel, fecha_inventario="",
                        tara=tara_bulk, observaciones="",
                    ))
                st.session_state["_procesando_bulk"] = False
                if not filas_bulk:
                    st.warning("No ingresaste cantidades mayores a 0.")
                else:
                    ok, msg = append_rows_con_retry(ws_his, filas_bulk)
                    if ok:
                        st.cache_data.clear()
                        st.success(f"Ingreso masivo registrado: {len(filas_bulk)} refs. {msg}")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)
    else:
        insumos_llegados = st.multiselect("🔍 Insumos recibidos:", sorted(nombres_ins))
        if insumos_llegados:
            regs_ingreso = {}
            st.divider()
            h1,h2,h3,h4,h5 = st.columns([3,2,1.5,1.5,2])
            for col, label in zip([h1,h2,h3,h4,h5],["Insumo","Stock Ant (Alm+Bar)","+ Cantidad","Tara","= Nuevo Total"]):
                col.write(f"**{label}**")
            st.divider()
            for i, nom in enumerate(insumos_llegados):
                row_matches = df_u[df_u["Nombre del Insumo"] == nom]
                if row_matches.empty: continue
                row_ins  = row_matches.iloc[0]
                prev     = buscar_insumo_en_actual(df_actual, nom)
                v_a_prev = prev["Alm"]   if prev is not None else 0.0
                v_b_prev = prev["Barra"] if prev is not None else 0.0
                v_min    = limpiar_valor(row_ins.get("Stock Mínimo",0))
                c1,c2,c3,c4,c5 = st.columns([3,2,1.5,1.5,2])
                with c1:
                    st.write(f"**{nom}**")
                    st.caption(f"Marca: {row_ins.get('Marca','-')} | Prov: {row_ins.get('Proveedor','-')}")
                with c2:
                    st.write(f"Almacén: {v_a_prev} | Barra: {v_b_prev}")
                    st.write(f"**Total Ant: {v_a_prev + v_b_prev}**")
                with c3:
                    cant_ingreso = st.number_input("Ingreso", min_value=0.0, step=1.0, value=None,
                                                   key=f"ing_{i}", label_visibility="collapsed", placeholder="0")
                    cant_ingreso = cant_ingreso if cant_ingreso is not None else 0.0
                with c4:
                    tara_ingreso = st.number_input("Tara", min_value=0.0, step=0.1, value=None,
                                                   key=f"tara_ing_{i}", label_visibility="collapsed", placeholder="tara")
                    tara_ingreso = tara_ingreso if tara_ingreso is not None else 0.0
                with c5:
                    cant_neta  = max(0.0, cant_ingreso - tara_ingreso)
                    nuevo_alm  = v_a_prev + cant_neta
                    nuevo_neto = nuevo_alm + v_b_prev
                    st.success(f"**{nuevo_neto:.1f}**")
                regs_ingreso[nom] = {"nuevo_a":nuevo_alm,"b":v_b_prev,"nuevo_n":nuevo_neto,"row":row_ins,"min":v_min,"tara":tara_ingreso}
                st.divider()

            proc_ing = st.session_state.get("_procesando_ingreso", False)
            btn_ing  = st.button("📦 EJECUTAR INGRESO", use_container_width=True, type="primary", disabled=proc_ing)
            if btn_ing and not proc_ing:
                st.session_state["_procesando_ingreso"] = True
                ws_his, err = safe_worksheet(sh, "Historial")
                if err:
                    st.error(err)
                    st.session_state["_procesando_ingreso"] = False
                else:
                    fh    = ts_hermosillo()
                    filas = []
                    for n, info in regs_ingreso.items():
                        dm = info["row"]
                        filas.append(construir_fila_historial(
                            unidad=u_sel, nombre=n, marca=dm.get("Marca",""),
                            proveedor=dm.get("Proveedor",""), grupo=dm.get("Grupo",""),
                            fecha_entrada=fh, presentacion=dm.get("Presentación de Compra",""),
                            unidad_medida=dm.get("Unidad de Medida","pz"),
                            alm=info["nuevo_a"], barra=info["b"], stock_neto=info["nuevo_n"],
                            stock_minimo=info["min"], comprar=info["nuevo_n"] < info["min"],
                            responsable=r_sel, fecha_inventario="", tara=info["tara"], observaciones="",
                        ))
                    ok, msg = append_rows_con_retry(ws_his, filas)
                    st.session_state["_procesando_ingreso"] = False
                    if ok:
                        st.cache_data.clear()
                        st.success(f"Ingreso registrado. {msg}")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)

# ── CONSULTA ─────────────────────────────────────────────────
elif pagina == "Consulta":
    df_raw, df_historial = cargar_datos_integrales()
    st.title("📦 Inventario actual")
    u_sel     = st.selectbox("🏢 Unidad:", UNIDADES)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    if df_actual.empty:
        st.warning("No hay registros en la base de datos para esta unidad.")
        st.stop()
    bajo_min = df_actual[df_actual["Necesita Compra"] == True]
    m1,m2,m3 = st.columns(3)
    m1.metric("Total Referencias", len(df_actual))
    m2.metric("Alertas de Compra", len(bajo_min), delta=-len(bajo_min), delta_color="inverse")
    m3.metric("Volumen Global",    f"{df_actual['Stock Neto Calculado'].sum():,.1f}")
    st.divider()
    col_s, col_p = st.columns([2,1])
    with col_s:
        busqueda = st.text_input("🔍 Búsqueda rápida:")
    with col_p:
        col_prov = "Proveedor" if "Proveedor" in df_actual.columns else None
        if col_prov:
            provs    = ["Todos"] + sorted(df_actual[col_prov].dropna().unique().tolist())
            prov_sel = st.selectbox("🚛 Filtro Proveedor:", provs)
        else:
            prov_sel = "Todos"
    df_display = df_actual.copy()
    if busqueda:
        df_display = df_display[df_display["Nombre del Insumo"].astype(str).str.contains(busqueda, case=False, na=False)]
    if prov_sel != "Todos" and col_prov:
        df_display = df_display[df_display[col_prov] == prov_sel]
    col_map = {
        "Grupo":"Grupo","Nombre del Insumo":"Insumo","Marca":"Marca","Proveedor":"Proveedor",
        "Alm":"Almacén","Barra":"Barra","Stock Neto Calculado":"Stock Total","Tara":"Tara",
        "Unidad de Medida":"Medida","Stock Mínimo":"Mínimo","Necesita Compra":"¿Comprar?",
        "Responsable":"Responsable","Fecha de Inventario":"Último Corte","Observaciones":"Observaciones",
    }
    cols_ok  = [c for c in col_map if c in df_display.columns]
    df_final = df_display[cols_ok].rename(columns=col_map)
    def highlight_low(row):
        total  = row.get("Stock Total",9999)
        minimo = row.get("Mínimo",0)
        color  = "background-color: rgba(255, 75, 75, 0.2)" if total < minimo else ""
        return [color] * len(row)
    st.dataframe(df_final.style.apply(highlight_low, axis=1), use_container_width=True, hide_index=True)
    st.divider()
    csv = df_final.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Descargar Reporte (CSV)", data=csv,
                       file_name=f"Inventario_{u_sel}_{ahora_hermosillo().strftime('%Y%m%d_%H%M')}.csv",
                       mime="text/csv", use_container_width=True)

# ── VENTAS — REGISTRO DIARIO ─────────────────────────────────
elif pagina == "Ventas":
    st.title("📈 Registrar Venta Diaria — Noble")
    mostrar_avisos()
    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida.")
        st.stop()

    df_ventas = cargar_ventas()
    hoy = ahora_hermosillo().date()

    ya_registrado = False
    if not df_ventas.empty and "Fecha" in df_ventas.columns:
        ya_registrado = any(f.date() == hoy for f in df_ventas["Fecha"].dropna())
    if ya_registrado:
        st.info(f"ℹ️ Ya existe un registro para hoy ({hoy.strftime('%d/%m/%Y')}). Puedes guardar una corrección si es necesario.")

    responsables = st.session_state.responsables or ["Raúl"]
    resp_idx = responsables.index(st.session_state.current_user) if st.session_state.current_user in responsables else 0

    col_f, col_r = st.columns([1,1])
    with col_f:
        fecha_venta = st.date_input("📅 Fecha del registro:", value=hoy, max_value=hoy)
    with col_r:
        responsable_v = st.selectbox("👤 Responsable:", responsables, index=resp_idx,
                                     disabled=(st.session_state.user_role != "admin"))

    st.divider()

    meta_default = 145000.0
    dias_default = 26
    if not df_ventas.empty:
        df_mes_actual = df_ventas[
            (df_ventas["Mes"].apply(limpiar_valor) == fecha_venta.month) &
            (df_ventas["Año"].apply(limpiar_valor) == fecha_venta.year)
        ]
        if not df_mes_actual.empty:
            meta_default = limpiar_valor(df_mes_actual["Meta_Mensual"].iloc[-1]) or meta_default
            dias_default = int(limpiar_valor(df_mes_actual["Dias_Habiles"].iloc[-1])) or dias_default

    with st.expander("⚙️ Configuración de Meta (mes actual)", expanded=False):
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            meta_mensual = st.number_input("Meta mensual ($):", min_value=0.0, step=1000.0, value=meta_default)
        with col_m2:
            dias_habiles = st.number_input("Días hábiles del mes:", min_value=1, max_value=31, value=dias_default)
        meta_diaria_calc = meta_mensual / dias_habiles if dias_habiles > 0 else 0
        st.caption(f"Meta diaria resultante: **${meta_diaria_calc:,.2f}**")

    st.subheader("💵 Venta del día")
    col_ef, col_tr, col_ta = st.columns(3)
    with col_ef: efectivo       = st.number_input("Efectivo ($):",       min_value=0.0, step=10.0, value=0.0)
    with col_tr: transferencias = st.number_input("Transferencias ($):", min_value=0.0, step=10.0, value=0.0)
    with col_ta: tarjeta        = st.number_input("Tarjeta ($):",        min_value=0.0, step=10.0, value=0.0)

    total_pos = efectivo + transferencias + tarjeta

    col_ub, col_rp = st.columns(2)
    with col_ub: uber  = st.number_input("Uber Eats ($):", min_value=0.0, step=10.0, value=0.0)
    with col_rp: rappi = st.number_input("Rappi ($):",     min_value=0.0, step=10.0, value=0.0)

    venta_total = total_pos + uber + rappi
    avance_pct  = (venta_total / meta_diaria_calc * 100) if meta_diaria_calc > 0 else 0

    st.divider()
    st.subheader("📊 Resumen en tiempo real")
    p1,p2,p3,p4 = st.columns(4)
    p1.metric("Total POS",   f"${total_pos:,.2f}")
    p2.metric("Plataformas", f"${uber + rappi:,.2f}")
    p3.metric("Venta Total", f"${venta_total:,.2f}")
    p4.metric("vs Meta día", f"{avance_pct:.1f}%", delta_color="normal" if avance_pct >= 100 else "inverse")

    st.divider()
    st.subheader("🎫 Tickets")
    col_tp, col_tu, col_tr2 = st.columns(3)
    with col_tp:  tickets_pos   = st.number_input("Tickets POS:",   min_value=0, step=1, value=0)
    with col_tu:  tickets_uber  = st.number_input("Tickets Uber:",  min_value=0, step=1, value=0)
    with col_tr2: tickets_rappi = st.number_input("Tickets Rappi:", min_value=0, step=1, value=0)

    total_tix = tickets_pos + tickets_uber + tickets_rappi
    tix_prom  = round(venta_total / total_tix, 2) if total_tix > 0 else 0.0

    t1, t2 = st.columns(2)
    t1.metric("Total Tickets",   total_tix)
    t2.metric("Ticket Promedio", f"${tix_prom:,.2f}" if tix_prom > 0 else "—")

    notas_v = st.text_input("📝 Notas del día (opcional):", placeholder="Ej: Día festivo, falla de sistema, etc.")

    # ── CAMBIO 1 ── Toggle para registrar día operativo con venta en cero ───────
    dia_sin_venta = st.toggle(
        "📵 Día sin venta (cierre en cero)",
        value=False,
        help="Activa esta opción para registrar un día operativo donde no hubo ventas. "
             "Permite distinguirlo de un día simplemente no capturado y mantiene tus promedios correctos."
    )
    if dia_sin_venta and venta_total == 0:
        st.warning("⚠️ Se registrará este día con venta = $0. Asegúrate de que la cafetería operó pero no tuvo ingresos.")
    # ── FIN CAMBIO 1 ─────────────────────────────────────────────────────────────

    st.divider()
    if st.button("💾 GUARDAR REGISTRO DE VENTA", type="primary", use_container_width=True):
        # ── CAMBIO 1 (continuación) ── Condición modificada para permitir día sin venta ─
        if venta_total == 0 and total_tix == 0 and not dia_sin_venta:
            st.warning("⚠️ Ingresa al menos un valor de venta o tickets, o activa 'Día sin venta' para registrar un cierre en cero.")
        # ── FIN CAMBIO 1 (continuación) ──────────────────────────────────────────────────
        else:
            ws_v, err = _asegurar_hoja_ventas()
            if err:
                st.error(err)
            else:
                # ── CAMBIO 1 (continuación) ── Inyectar nota automática si día sin venta ─
                notas_final = notas_v if notas_v.strip() else ("DÍA SIN VENTA" if dia_sin_venta else "")
                # ── FIN CAMBIO 1 (continuación) ──────────────────────────────────────────
                fila = _construir_fila_venta(
                    fecha=fecha_venta, efectivo=efectivo, transferencias=transferencias,
                    tarjeta=tarjeta, uber=uber, rappi=rappi,
                    tickets_pos=tickets_pos, tickets_uber=tickets_uber, tickets_rappi=tickets_rappi,
                    meta_mensual=meta_mensual, dias_habiles=int(dias_habiles),
                    responsable=responsable_v, notas=notas_final,
                )
                ok, msg = append_rows_con_retry(ws_v, [fila])
                if ok:
                    st.cache_data.clear()
                    st.success(f"✅ Venta del {fecha_venta.strftime('%d/%m/%Y')} registrada. Total: ${venta_total:,.2f}")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error(msg)

# ── VENTAS — DASHBOARD ────────────────────────────────────────
elif pagina == "DashboardVentas":
    st.title("📊 Dashboard de Ventas — Noble")
    df_v = cargar_ventas()
    if df_v.empty:
        st.info("Sin registros de venta. Comienza capturando el primer día.")
        st.stop()

    meses_disp = sorted(
        df_v[["Mes","Año"]].drop_duplicates().apply(
            lambda r: (int(limpiar_valor(r["Mes"])), int(limpiar_valor(r["Año"]))), axis=1
        ).tolist(), reverse=True
    )
    meses_disp = [(m,a) for m,a in meses_disp if m > 0 and a > 0]
    opciones_mes = [f"{calendar.month_name[m].capitalize()} {a}" for m,a in meses_disp]
    mes_sel_str = st.selectbox("📅 Mes:", opciones_mes) if opciones_mes else None
    if not mes_sel_str:
        st.info("Sin datos de mes disponibles.")
        st.stop()

    mes_idx = opciones_mes.index(mes_sel_str)
    mes_num, año_num = meses_disp[mes_idx]
    df_mes = df_v[
        (df_v["Mes"].apply(limpiar_valor) == mes_num) &
        (df_v["Año"].apply(limpiar_valor) == año_num)
    ].copy().sort_values("Fecha")

    if df_mes.empty:
        st.warning("Sin registros para ese mes.")
        st.stop()

    meta_m     = limpiar_valor(df_mes["Meta_Mensual"].iloc[-1])
    dias_hab   = int(limpiar_valor(df_mes["Dias_Habiles"].iloc[-1])) or 1
    venta_acum = df_mes["Venta_Diaria"].sum()
    tix_total  = int(df_mes["Total_Tickets"].sum())
    tix_prom_g = round(venta_acum / tix_total, 2) if tix_total > 0 else 0
    faltante   = meta_m - venta_acum
    avance_pct = (venta_acum / meta_m * 100) if meta_m > 0 else 0

    # ── CAMBIO 2 ── Cálculo de días con/sin venta para métricas precisas ────────
    dias_con_venta_cnt  = int((df_mes["Venta_Diaria"] > 0).sum())
    dias_sin_venta_cnt  = int((df_mes["Venta_Diaria"] == 0).sum())
    # Ticket promedio real: excluye días con tickets = 0 para no distorsionar el promedio
    df_con_tix          = df_mes[df_mes["Total_Tickets"] > 0]
    tix_acum_con_venta  = int(df_con_tix["Total_Tickets"].sum())
    venta_acum_con_tix  = df_con_tix["Venta_Diaria"].sum()
    tix_prom_real       = round(venta_acum_con_tix / tix_acum_con_venta, 2) if tix_acum_con_venta > 0 else 0
    # ── FIN CAMBIO 2 ─────────────────────────────────────────────────────────────

    st.subheader(f"Resumen — {mes_sel_str}")
    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Venta Acumulada", f"${venta_acum:,.2f}")
    k2.metric("Meta Mensual",    f"${meta_m:,.2f}")
    k3.metric("Faltante",        f"${faltante:,.2f}", delta=f"{avance_pct:.1f}% avance",
              delta_color="normal" if faltante <= 0 else "inverse")
    k4.metric("Ticket Promedio", f"${tix_prom_g:,.2f}")

    # ── CAMBIO 2 (continuación) ── Fila de métricas de tickets y cobertura diaria ─
    st.divider()
    st.subheader("🎫 Métricas de Tickets")
    tk1, tk2, tk3, tk4 = st.columns(4)
    tk1.metric(
        "Tickets Acumulados",
        f"{tix_total:,}",
        help="Total de transacciones registradas en el mes (POS + Uber + Rappi)."
    )
    tk2.metric(
        "Ticket Promedio Real",
        f"${tix_prom_real:,.2f}" if tix_prom_real > 0 else "—",
        help="Promedio calculado únicamente sobre días con al menos un ticket. "
             "Excluye días sin venta para no distorsionar el indicador."
    )
    tk3.metric(
        "Días con Venta",
        f"{dias_con_venta_cnt}",
        delta=f"de {len(df_mes)} registrados",
        delta_color="off",
        help="Días del mes donde se registró al menos un peso de venta."
    )
    tk4.metric(
        "Días sin Venta",
        f"{dias_sin_venta_cnt}",
        delta_color="inverse" if dias_sin_venta_cnt > 0 else "off",
        help="Días registrados explícitamente con venta = $0. "
             "Un valor mayor a 0 indica días operativos sin ingresos (no días sin captura)."
    )
    # ── FIN CAMBIO 2 (continuación) ──────────────────────────────────────────────

    st.divider()
    if not df_mes.empty:
        mejor = df_mes.loc[df_mes["Venta_Diaria"].idxmax()]
        df_con_venta = df_mes[df_mes["Venta_Diaria"] > 0]
        peor = df_con_venta.loc[df_con_venta["Venta_Diaria"].idxmin()] if not df_con_venta.empty else None
        b1,b2,b3 = st.columns(3)
        b1.metric("📈 Mejor día", f"${limpiar_valor(mejor['Venta_Diaria']):,.0f}", f"Día {int(limpiar_valor(mejor['Día']))}")
        if peor is not None:
            b2.metric("📉 Día más bajo (con venta)", f"${limpiar_valor(peor['Venta_Diaria']):,.0f}", f"Día {int(limpiar_valor(peor['Día']))}")
        b3.metric("📅 Días registrados", len(df_mes))

    st.divider()
    st.subheader("📋 Detalle diario")
    df_disp = df_mes[["Día","Fecha","Efectivo","Transferencias","Tarjeta","Total_POS",
                       "Uber_Eats","Rappi","Venta_Diaria","Total_Tickets",
                       "Ticket_Promedio","Meta_Diaria","Responsable","Notas"]].copy()
    df_disp["Fecha"] = df_disp["Fecha"].apply(lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "")
    df_disp["vs Meta"] = df_disp.apply(
        lambda r: f"{(r['Venta_Diaria']/r['Meta_Diaria']*100):.0f}%" if r['Meta_Diaria'] > 0 else "—", axis=1
    )
    df_disp["Ticket_Promedio"] = df_disp["Ticket_Promedio"].apply(lambda x: f"${x:,.2f}" if x > 0 else "—")

    def color_meta_row(row):
        try:
            vd = limpiar_valor(row.get("Venta_Diaria",0))
            md = limpiar_valor(row.get("Meta_Diaria",0))
            if md == 0: return [""] * len(row)
            ratio = vd / md
            c = ("background-color: rgba(80,200,120,0.15)" if ratio >= 1.0
                 else "background-color: rgba(239,159,39,0.15)" if ratio >= 0.7
                 else "background-color: rgba(226,75,74,0.12)")
            return [c] * len(row)
        except Exception:
            return [""] * len(row)

    st.dataframe(df_disp.style.apply(color_meta_row, axis=1), hide_index=True, use_container_width=True)

    st.divider()
    st.subheader("🥧 Mix de canales")
    tot_pos  = df_mes["Total_POS"].sum()
    tot_uber = df_mes["Uber_Eats"].sum()
    tot_rapp = df_mes["Rappi"].sum()
    tot_all  = tot_pos + tot_uber + tot_rapp or 1
    c_pos, c_uber, c_rapp = st.columns(3)
    c_pos.metric( "POS",       f"${tot_pos:,.2f}",  f"{tot_pos/tot_all*100:.1f}%")
    c_uber.metric("Uber Eats", f"${tot_uber:,.2f}", f"{tot_uber/tot_all*100:.1f}%")
    c_rapp.metric("Rappi",     f"${tot_rapp:,.2f}", f"{tot_rapp/tot_all*100:.1f}%")

    st.divider()
    csv_v = df_disp.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Descargar CSV del mes", data=csv_v,
                       file_name=f"Ventas_Noble_{mes_sel_str.replace(' ','_')}.csv",
                       mime="text/csv", use_container_width=True)

# ── VENTAS — IMPORTAR HISTÓRICO ──────────────────────────────
elif pagina == "ImportarVentas":
    st.title("📥 Importar Histórico de Ventas")
    mostrar_avisos()
    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida.")
        st.stop()

    st.info("Sube el Excel mensual con el formato estándar Noble. El sistema parsea automáticamente y guarda en Sheets.")

    col_imp1, col_imp2 = st.columns(2)
    with col_imp1:
        mes_imp = st.selectbox("Mes del archivo:", list(range(1,13)),
                               format_func=lambda m: calendar.month_name[m].capitalize(),
                               index=ahora_hermosillo().month - 1)
    with col_imp2:
        año_imp = st.number_input("Año:", min_value=2023, max_value=2030, value=ahora_hermosillo().year)

    col_meta1, col_meta2 = st.columns(2)
    with col_meta1:
        meta_imp = st.number_input("Meta mensual ($):", min_value=0.0, step=1000.0, value=145000.0)
    with col_meta2:
        dias_imp = st.number_input("Días hábiles del mes:", min_value=1, max_value=31, value=26)

    archivo = st.file_uploader("📂 Selecciona el archivo Excel (.xlsx):", type=["xlsx"])

    if archivo:
        try:
            df_raw_imp = pd.read_excel(archivo, sheet_name=0, header=None)
            filas_datos = []
            for _, fila in df_raw_imp.iterrows():
                try:
                    dia = int(float(str(fila.iloc[1]).strip()))
                    if 1 <= dia <= 31:
                        filas_datos.append(fila)
                except (ValueError, TypeError):
                    continue

            if not filas_datos:
                st.error("No se encontraron filas de datos válidas en el archivo.")
                st.stop()

            df_parse = pd.DataFrame(filas_datos)
            df_parse.columns = range(df_parse.shape[1])

            def _n(v):
                try:
                    f = float(str(v).strip())
                    return 0.0 if str(v).strip() in ['nan',''] else (0.0 if f != f else f)
                except Exception:
                    return 0.0

            filas_import    = []
            dias_sin_venta  = []

            for _, row in df_parse.iterrows():
                dia = int(_n(row.iloc[1]))
                if dia < 1 or dia > 31: continue
                efectivo_i       = _n(row.iloc[2])
                transferencias_i = _n(row.iloc[3])
                tarjeta_i        = _n(row.iloc[4])
                uber_i           = _n(row.iloc[6])
                rappi_i          = _n(row.iloc[7])
                tickets_pos_i    = int(_n(row.iloc[10]))
                tickets_uber_i   = int(_n(row.iloc[11]))
                tickets_rappi_i  = int(_n(row.iloc[12]))
                venta_d = efectivo_i + transferencias_i + tarjeta_i + uber_i + rappi_i
                if venta_d == 0 and tickets_pos_i == 0:
                    dias_sin_venta.append(dia)
                    continue
                try:
                    fecha_d = _date(int(año_imp), int(mes_imp), dia)
                except ValueError:
                    continue
                filas_import.append(_construir_fila_venta(
                    fecha=fecha_d, efectivo=efectivo_i, transferencias=transferencias_i,
                    tarjeta=tarjeta_i, uber=uber_i, rappi=rappi_i,
                    tickets_pos=tickets_pos_i, tickets_uber=tickets_uber_i,
                    tickets_rappi=tickets_rappi_i, meta_mensual=meta_imp,
                    dias_habiles=int(dias_imp), responsable="IMPORTADO", notas="",
                ))

            if filas_import:
                cols_prev = ["Fecha","Efectivo","Transferencias","Tarjeta","Total_POS",
                             "Uber_Eats","Rappi","Venta_Diaria","Total_Tickets","Ticket_Promedio"]
                idx_prev  = {c:i for i,c in enumerate(COLS_VENTAS)}
                rows_prev = [[f[idx_prev[c]] for c in cols_prev] for f in filas_import]
                df_prev   = pd.DataFrame(rows_prev, columns=cols_prev)
                total_imp = df_prev["Venta_Diaria"].apply(limpiar_valor).sum()

                st.success(f"✅ {len(filas_import)} día(s) con venta detectados. Venta acumulada: **${total_imp:,.2f}**")
                if dias_sin_venta:
                    st.caption(f"Días sin venta (omitidos): {dias_sin_venta}")
                st.dataframe(df_prev, hide_index=True, use_container_width=True)

                if st.button("📤 GUARDAR EN GOOGLE SHEETS", type="primary", use_container_width=True):
                    ws_v, err = _asegurar_hoja_ventas()
                    if err:
                        st.error(err)
                    else:
                        ok, msg = append_rows_con_retry(ws_v, filas_import)
                        if ok:
                            st.cache_data.clear()
                            st.success(f"Histórico importado: {len(filas_import)} registros guardados.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)
            else:
                st.warning("No se encontraron días con venta en el archivo.")

        except Exception as e:
            st.error(f"Error al procesar el archivo: {e}")
            st.exception(e)

# ── IMPRESIÓN ─────────────────────────────────────────────────
elif pagina == "Impresion":
    df_raw, _ = cargar_datos_integrales()
    st.title("🖨️ Ticket de Conteo (58mm)")
    u_sel = st.selectbox("Sucursal:", UNIDADES)
    # ← NUEVO: df_raw ya viene filtrado (solo activos), no se requiere cambio aquí
    df_u  = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    grps  = sorted(df_u["Grupo"].dropna().unique().tolist()) if not df_u.empty and "Grupo" in df_u.columns else []
    g_sel = st.multiselect("Filtrar por Grupos:", grps)

    if g_sel and not df_u.empty:
        df_p = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo","Nombre del Insumo"])
        lineas_pdf = [
            (f"* CONTEO {u_sel.upper()} *", "title"),
            (f"Fecha: {ahora_hermosillo().strftime('%d/%m/%Y')}", "small"),
            ("", "divider"),
        ]
        gr_actual = ""
        for _, r in df_p.iterrows():
            grupo = str(r.get("Grupo",""))
            if grupo != gr_actual:
                lineas_pdf.append((f">> GRUPO {grupo} <<", "bold"))
                gr_actual = grupo
            lineas_pdf.append((str(r['Nombre del Insumo'])[:22], "normal"))
            lineas_pdf.append(("[    ] Alm   [    ] Bar", "small"))
            lineas_pdf.append(("", "divider"))

        with st.expander("👁️ Vista previa del contenido", expanded=True):
            prev_txt = f"{'='*28}\n* CONTEO {u_sel.upper()} *\nFecha: {ahora_hermosillo().strftime('%d/%m/%Y')}\n{'-'*28}\n"
            gr_actual_p = ""
            for _, r in df_p.iterrows():
                grupo = str(r.get("Grupo",""))
                if grupo != gr_actual_p:
                    prev_txt += f"\n>> GRUPO {grupo} <<\n"
                    gr_actual_p = grupo
                prev_txt += f" {str(r['Nombre del Insumo'])[:22]}\n [    ] Alm   [    ] Bar\n{'-'*28}\n"
            st.code(prev_txt, language=None)

        pdf_bytes = generar_pdf_58mm(f"Conteo {u_sel}", lineas_pdf)
        st.download_button(
            label="📄 Descargar PDF 58mm", data=pdf_bytes,
            file_name=f"conteo_{u_sel.replace(' ','_')}_{ahora_hermosillo().strftime('%Y%m%d_%H%M')}.pdf",
            mime="application/pdf", use_container_width=True, type="primary"
        )
    else:
        st.info("Selecciona grupos para generar la lista.")

# ── LISTA DE COMPRA ───────────────────────────────────────────
elif pagina == "ListaCompra":
    _, df_historial = cargar_datos_integrales()
    st.title("🛒 Ticket de Compra (58mm)")
    u_sel     = st.radio("Generar orden para:", UNIDADES, horizontal=True)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    if df_actual.empty:
        st.info("Sin registros para armar la lista de compra.")
        st.stop()
    com = df_actual[df_actual["Necesita Compra"] == True]
    if not com.empty:
        lineas_pdf = [
            (f"* COMPRAS {u_sel.upper()} *", "title"),
            (f"Fecha: {ahora_hermosillo().strftime('%d/%m/%Y')}", "small"),
            ("", "divider"),
        ]
        for _, r in com.iterrows():
            lineas_pdf.append((f"* {str(r['Nombre del Insumo'])[:22]}", "bold"))
            lineas_pdf.append((f"  Stock:{r['Stock Neto Calculado']} Min:{r['Stock Mínimo']}", "small"))
            lineas_pdf.append(("", "divider"))
        with st.expander("👁️ Vista previa del contenido", expanded=True):
            prev_txt = f"{'='*28}\n* COMPRAS {u_sel.upper()} *\nFecha: {ahora_hermosillo().strftime('%d/%m/%Y')}\n{'-'*28}\n"
            for _, r in com.iterrows():
                prev_txt += f"• {str(r['Nombre del Insumo'])[:22]}\n  Stock: {r['Stock Neto Calculado']} / Min: {r['Stock Mínimo']}\n{'-'*28}\n"
            st.code(prev_txt, language=None)
        pdf_bytes = generar_pdf_58mm(f"Compras {u_sel}", lineas_pdf)
        st.download_button(
            label="📄 Descargar PDF 58mm", data=pdf_bytes,
            file_name=f"compras_{u_sel.replace(' ','_')}_{ahora_hermosillo().strftime('%Y%m%d_%H%M')}.pdf",
            mime="application/pdf", use_container_width=True, type="primary"
        )
    else:
        st.success("No hay alertas de reabastecimiento activas.")

# ── REPORTE DE STOCK ──────────────────────────────────────────
elif pagina == "ReporteStock":
    _, df_historial = cargar_datos_integrales()
    st.title("📦 Reporte de Stock (58mm)")
    u_sel     = st.radio("Generar reporte para:", UNIDADES, horizontal=True)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    if df_actual.empty:
        st.warning("Sin registros para generar el reporte.")
        st.stop()
    df_rep = df_actual.sort_values(["Grupo","Nombre del Insumo"])
    lineas_pdf = [
        (f"* INVENTARIO {u_sel.upper()} *", "title"),
        (ahora_hermosillo().strftime('%d/%m/%Y %H:%M'), "small"),
        ("", "divider"),
    ]
    gr_actual = ""
    for _, r in df_rep.iterrows():
        grupo = str(r.get("Grupo",""))
        if grupo != gr_actual:
            lineas_pdf.append((f">> GRUPO {grupo} <<", "bold"))
            gr_actual = grupo
        lineas_pdf.append((str(r['Nombre del Insumo'])[:20], "normal"))
        lineas_pdf.append((f" Alm:{r['Alm']} Bar:{r['Barra']} Tot:{r['Stock Neto Calculado']}", "small"))
    lineas_pdf.append(("", "divider"))

    with st.expander("👁️ Vista previa del contenido", expanded=True):
        prev_txt = f"{'='*28}\n* INVENTARIO {u_sel.upper()} *\n{ahora_hermosillo().strftime('%d/%m/%Y %H:%M')}\n{'-'*28}\n"
        gr_actual_p = ""
        for _, r in df_rep.iterrows():
            grupo = str(r.get("Grupo",""))
            if grupo != gr_actual_p:
                prev_txt += f"\n>> GRUPO {grupo} <<\n"
                gr_actual_p = grupo
            prev_txt += f"{str(r['Nombre del Insumo'])[:20]}\n Alm:{r['Alm']} Bar:{r['Barra']} Total:{r['Stock Neto Calculado']}\n"
        prev_txt += "-" * 28 + "\n"
        st.code(prev_txt, language=None)

    pdf_bytes = generar_pdf_58mm(f"Stock {u_sel}", lineas_pdf)
    st.download_button(
        label="📄 Descargar PDF 58mm", data=pdf_bytes,
        file_name=f"stock_{u_sel.replace(' ','_')}_{ahora_hermosillo().strftime('%Y%m%d_%H%M')}.pdf",
        mime="application/pdf", use_container_width=True, type="primary"
    )

# ── CORTE DE MES ──────────────────────────────────────────────
elif pagina == "CorteMes":
    _, df_historial = cargar_datos_integrales()
    if st.session_state.user_role != "admin":
        st.error("🚫 Acceso denegado. Solo administradores.")
        st.stop()
    st.title("🔒 Corte de Mes")
    st.warning(
        "Este proceso consolidará el stock actual como saldo inicial y archivará "
        "los registros previos. **Acción irreversible.** "
        "Asegúrate de que todos los conteos del día estén registrados antes de continuar."
    )
    confirmar = st.checkbox("Confirmo que deseo ejecutar el cierre de mes.")
    if confirmar and st.button("🚀 Ejecutar Cierre", type="primary"):
        with st.status("Ejecutando protocolo de cierre...", expanded=True) as status:
            try:
                st.write("1/4 — Calculando estados finales de stock...")
                df_corte = obtener_ultimo_inventario(df_historial)
                if df_corte.empty:
                    st.error("No hay datos de inventario para cerrar.")
                    st.stop()
                fh          = ts_hermosillo()
                encabezados = COLS_HISTORIAL
                filas_corte = []
                for _, r in df_corte.iterrows():
                    filas_corte.append(construir_fila_historial(
                        unidad=r.get("Unidad de Negocio",""), nombre=r.get("Nombre del Insumo",""),
                        marca=r.get("Marca",""), proveedor=r.get("Proveedor",""),
                        grupo=r.get("Grupo",""), fecha_entrada="",
                        presentacion=r.get("Presentación de Compra",""),
                        unidad_medida=r.get("Unidad de Medida",""),
                        alm=r.get("Alm",0), barra=r.get("Barra",0),
                        stock_neto=r.get("Stock Neto Calculado",0), stock_minimo=r.get("Stock Mínimo",0),
                        comprar=bool(r.get("Necesita Compra",False)), responsable="SISTEMA-CIERRE",
                        fecha_inventario=fh, tara=r.get("Tara",0), observaciones="Corte consolidado",
                    ))
                st.write("2/4 — Archivando historial previo...")
                ws_his, err = safe_worksheet(sh, "Historial")
                if err: raise RuntimeError(err)
                datos_hist = ws_his.get_all_values()
                if len(datos_hist) <= 1:
                    st.warning("El Historial ya está vacío.")
                    status.update(label="⚠️ Historial ya estaba vacío", state="error")
                    st.stop()
                ws_arc, _ = safe_worksheet(sh, "Archivo_Historial")
                if ws_arc is None:
                    ws_arc = sh.add_worksheet(title="Archivo_Historial", rows="10000", cols="20")
                    ws_arc.append_row(encabezados)
                ws_arc.append_row([f"=== CORTE {fh} ==="] + [""] * (len(encabezados) - 1))
                ws_arc.append_rows(datos_hist[1:])
                st.write("3/4 — Consolidando saldos iniciales...")
                ws_cie, _ = safe_worksheet(sh, "Cierres")
                if ws_cie is None:
                    ws_cie = sh.add_worksheet(title="Cierres", rows="1000", cols="20")
                ws_cie.clear()
                ws_cie.append_row(encabezados)
                ws_cie.append_rows(filas_corte)
                st.write("4/4 — Reiniciando Historial...")
                ws_his.clear()
                ws_his.append_row(encabezados)
                st.cache_data.clear()
                status.update(label="✅ Cierre completado", state="complete")
                st.success(f"{len(filas_corte)} referencias consolidadas en 'Cierres'.")
                time.sleep(2)
                st.rerun()
            except Exception as e:
                status.update(label="❌ Falla en el cierre", state="error")
                st.error(f"Error durante el cierre: {e}\n\nEl Historial NO fue eliminado.")
