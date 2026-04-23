import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime, timezone, timedelta
import time
import calendar
import unicodedata
import re
import io
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.lib.units import mm

st.set_page_config(layout="wide")

# ============================================================
# CONSTANTES — única fuente de verdad para todo el sistema
# Para agregar una columna: editar aquí y en el Sheet.
# NUNCA usar strings literales de columnas fuera de este bloque.
# ============================================================
COLS_INSUMOS = [
    "Unidad de Negocio",      # A
    "Nombre del Insumo",      # B
    "Marca",                  # C
    "Proveedor",              # D
    "Grupo",                  # E
    "Espacio_1",              # F — reservado / fecha entrada
    "Presentación de Compra", # G
    "Unidad de Medida",       # H
    "Espacio_2",              # I
    "Espacio_3",              # J
    "Espacio_4",              # K
    "Stock Mínimo",           # L
    "Tara",                   # M — peso de tara del contenedor
]
COLS_HISTORIAL = [
    "Unidad de Negocio",      # A
    "Nombre del Insumo",      # B
    "Marca",                  # C  — nombre real en el Sheet
    "Proveedor",              # D  — nombre real en el Sheet
    "Grupo",                  # E  — nombre real en el Sheet
    "Fecha de Entrada",       # F  — nombre real en el Sheet
    "Presentación de Compra", # G  — nombre real en el Sheet
    "Unidad de Medida",       # H  — nombre real en el Sheet
    "Alm",                    # I
    "Barra",                  # J
    "Stock Neto",             # K
    "Stock Mínimo",           # L
    "¿Comprar?",              # M  — nombre real en el Sheet
    "Responsable",            # N
    "Fecha de Inventario",    # O
    "Observaciones",          # P  — nombre real en el Sheet
]
COLS_ACCESOS    = ["Clave", "Nombre", "Rol"]

# Columnas críticas: si faltan, el sistema no puede funcionar correctamente
COLS_CRITICAS_INSUMOS   = {"Nombre del Insumo", "Grupo", "Stock Mínimo"}
COLS_CRITICAS_HISTORIAL = {"Nombre del Insumo", "Alm", "Barra", "Fecha de Inventario"}

GRUPOS       = ["A", "B", "C", "D", "E", "F", "G"]
UNIDADES     = ["Noble", "Coffee Station"]
UNIDADES_MED = ["pz", "ml", "gr", "kg", "lt"]

SPREADSHEET_ID = "1VZV81p-JqoaRPzMzsRurF6wntVefyaN5ozs3RJe6uJs"

# ============================================================
# ZONA HORARIA — Hermosillo usa MST fijo (UTC-7), sin horario de verano
# ============================================================
TZ_HERMOSILLO = timezone(timedelta(hours=-7))

def ahora_hermosillo() -> datetime:
    """Retorna datetime actual en hora de Hermosillo (MST, UTC-7)."""
    return datetime.now(tz=TZ_HERMOSILLO)

def ts_hermosillo() -> str:
    """Timestamp formateado para guardar en Sheets."""
    return ahora_hermosillo().strftime("%Y-%m-%d %H:%M:%S")

def fmt_fecha_hmo(dt) -> str:
    """
    Formatea un timestamp (naive o aware) a hora de Hermosillo.
    Si el valor es NaT/None retorna cadena vacía.
    """
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
            # Asumir que Sheets guarda en UTC si no tiene zona
            dt = dt.replace(tzinfo=timezone.utc)
        dt_hmo = dt.astimezone(TZ_HERMOSILLO)
        return dt_hmo.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(dt)[:16]

# ============================================================
# HELPERS UTILITARIOS
# ============================================================

def limpiar_valor(valor) -> float:
    """
    Convierte cualquier valor de celda a float de forma segura.
    Retorna 0.0 solo si el valor realmente es vacío/nulo/inválido.

    Mejoras v3 (integración Gemini):
    - Maneja booleanos nativos (True → 1.0, False → 0.0)
    - Elimina espacios internos dentro del número ("1 500" → 1500.0)
    - Tolera guiones como cero ("-" → 0.0)
    - Maneja notación científica ("1.5e3" → 1500.0)
    - Maneja NaN de pandas y float('nan')
    """
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
        # Eliminar símbolos de moneda/porcentaje y espacios internos
        s = s.replace('%', '').replace('$', '').replace(',', '').replace(' ', '')
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def normalizar_nombre(nombre) -> str:
    """
    Normaliza un nombre de insumo para comparaciones robustas.
    Elimina acentos, espacios extra y convierte a minúsculas.
    "Café Molido " == "cafe molido" == "CAFÉ MOLIDO"
    """
    s = str(nombre).strip().lower()
    # Quitar acentos/diacríticos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Colapsar espacios múltiples
    s = re.sub(r'\s+', ' ', s)
    return s


def normalizar_dataframe(df: pd.DataFrame, columnas_esperadas: list,
                         cols_criticas: set = None) -> pd.DataFrame:
    """
    Mapea columnas por NOMBRE (no por posición).
    - Si el encabezado del Sheet coincide con la constante: mapeo directo.
    - Columnas que faltan: se crean como None con advertencia si son críticas.
    - Columnas extra en el Sheet: se ignoran (no corrompen datos).

    Esta función es inmune a reorganizaciones de columnas en el Sheet.
    """
    if df.empty:
        return pd.DataFrame(columns=columnas_esperadas)

    df = df.copy()
    cols_en_sheet    = set(df.columns)
    cols_faltantes   = [c for c in columnas_esperadas if c not in cols_en_sheet]
    cols_presentes   = [c for c in columnas_esperadas if c in cols_en_sheet]

    # Advertir si faltan columnas críticas
    if cols_criticas:
        faltantes_criticas = set(cols_faltantes) & cols_criticas
        if faltantes_criticas:
            st.warning(
                f"⚠️ Columnas críticas no encontradas en el Sheet: {sorted(faltantes_criticas)}. "
                "Verifica que los encabezados del Sheet coincidan exactamente con la configuración."
            )

    # Completar columnas faltantes con None
    for col in cols_faltantes:
        df[col] = None

    # Devolver solo las columnas esperadas en el orden esperado
    return df[columnas_esperadas]


def safe_worksheet(sh, nombre: str):
    """
    Obtiene un worksheet con validación explícita.
    Retorna (worksheet, None) o (None, mensaje_de_error).
    """
    if sh is None:
        return None, "Sin conexión activa a Google Sheets."
    try:
        return sh.worksheet(nombre), None
    except gspread.exceptions.WorksheetNotFound:
        return None, f"Pestaña '{nombre}' no encontrada en el Spreadsheet."
    except Exception as e:
        return None, f"Error accediendo a '{nombre}': {e}"


def append_rows_con_retry(worksheet, filas: list, max_intentos: int = 3) -> tuple:
    """
    Escribe filas con reintentos y backoff exponencial ante errores de quota.
    Retorna (éxito: bool, mensaje: str).
    """
    if not filas:
        return False, "No hay filas para escribir."
    for intento in range(1, max_intentos + 1):
        try:
            worksheet.append_rows(filas, value_input_option="USER_ENTERED")
            return True, f"{len(filas)} fila(s) registrada(s)."
        except gspread.exceptions.APIError as e:
            codigo = getattr(e.response, 'status_code', 0)
            if codigo == 429 and intento < max_intentos:
                espera = 2 ** intento
                time.sleep(espera)
                continue
            return False, f"Error de API Sheets (intento {intento}/{max_intentos}): {e}"
        except Exception as e:
            return False, f"Error inesperado al escribir en Sheets: {e}"
    return False, "Se agotaron los reintentos de escritura."


def bloquear_doble_envio(key: str):
    """
    Marca una operación como 'en curso' en session_state.
    Devuelve True si ya hay una operación activa (bloquear).
    Uso: if bloquear_doble_envio('inv'): st.stop()
    """
    flag = f"_enviando_{key}"
    if st.session_state.get(flag, False):
        st.warning("⏳ Operación en curso, espera un momento...")
        return True
    st.session_state[flag] = True
    return False


def liberar_doble_envio(key: str):
    """Libera el flag de bloqueo de doble envío."""
    st.session_state.pop(f"_enviando_{key}", None)


# ============================================================
# GENERADOR DE PDF 58mm
# ============================================================
def generar_pdf_58mm(titulo: str, lineas: list) -> bytes:
    """
    Genera un PDF con ancho de 58mm (rollo térmico).
    lineas: lista de strings o tuplas (texto, estilo) donde estilo puede ser
            'normal', 'bold', 'small', 'divider'
    Retorna bytes del PDF.
    """
    ANCHO_MM = 58
    MARGEN_MM = 3
    LINEA_H_MM = 4.2
    FUENTE_NORMAL = 7.5
    FUENTE_BOLD = 8
    FUENTE_SMALL = 6.5

    # Calcular altura necesaria
    alto_mm = 20 + len(lineas) * LINEA_H_MM + 10
    alto_mm = max(alto_mm, 40)

    ancho_pts = ANCHO_MM * mm
    alto_pts = alto_mm * mm
    margen_pts = MARGEN_MM * mm

    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=(ancho_pts, alto_pts))
    c.setFont("Courier-Bold", FUENTE_BOLD)

    y = alto_pts - (5 * mm)

    for linea in lineas:
        if isinstance(linea, tuple):
            texto, estilo = linea
        else:
            texto, estilo = linea, 'normal'

        if estilo == 'divider':
            c.setFont("Courier", FUENTE_SMALL)
            c.drawString(margen_pts, y, "-" * 30)
            y -= LINEA_H_MM * mm
            continue
        elif estilo == 'bold':
            c.setFont("Courier-Bold", FUENTE_BOLD)
        elif estilo == 'small':
            c.setFont("Courier", FUENTE_SMALL)
        elif estilo == 'title':
            c.setFont("Courier-Bold", FUENTE_BOLD + 1)
        else:
            c.setFont("Courier", FUENTE_NORMAL)

        # Truncar texto para que no desborde el ancho
        max_chars = int((ANCHO_MM - MARGEN_MM * 2) / (FUENTE_NORMAL * 0.6))
        texto_trunc = str(texto)[:max_chars]
        c.drawString(margen_pts, y, texto_trunc)
        y -= LINEA_H_MM * mm

        if y < (5 * mm):
            c.showPage()
            y = alto_pts - (5 * mm)

    c.save()
    buf.seek(0)
    return buf.read()


# ============================================================
# CAPA DE CONEXIÓN
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
# CARGA DE DATOS
# CORRECCIÓN E: cargar_datos_integrales() se llama desde cada
# página, no se asigna a nivel de módulo. El caché de 30s
# absorbe las llamadas repetidas sin costo de API extra.
# ============================================================
@st.cache_data(ttl=30)
def cargar_datos_integrales():
    """
    Carga Insumos + Historial + Cierres desde Google Sheets.
    TTL=30s: tras cache_data.clear() + rerun(), los datos
    se recargan correctamente en todos los usuarios activos.
    """
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

        # Preservar número de fila ANTES de normalizar (normalizar reordena columnas)
        df_ins["Sheet_Row_Num"] = df_ins.index + 2

        # CORRECCIÓN A: normalizar por nombre, no por posición
        df_ins = normalizar_dataframe(df_ins, COLS_INSUMOS + ["Sheet_Row_Num"],
                                      cols_criticas=COLS_CRITICAS_INSUMOS)
        df_his = normalizar_dataframe(df_his, COLS_HISTORIAL,
                                      cols_criticas=COLS_CRITICAS_HISTORIAL)

        if not df_cie.empty:
            df_cie   = normalizar_dataframe(df_cie, COLS_HISTORIAL)
            df_total = pd.concat([df_cie, df_his], ignore_index=True)
        else:
            df_total = df_his

        if not df_total.empty:
            df_total["Fecha de Inventario"] = pd.to_datetime(
                df_total["Fecha de Inventario"], errors="coerce"
            )
            # "Fecha de Entrada" se usa como fecha de respaldo solo si es parseable
            df_total["Fecha de Entrada"] = pd.to_datetime(
                df_total["Fecha de Entrada"], errors="coerce"
            )

            # ── MERGE MAESTRO: datos estáticos SIEMPRE de Insumos ────────────
            # Insumos  → fuente de verdad para: Nombre, Marca, Proveedor,
            #            Grupo, Presentación de Compra, Unidad de Medida,
            #            Stock Mínimo, Tara.
            # Historial → fuente de verdad para: Alm, Barra, Stock Neto,
            #             ¿Comprar?, Responsable, Fecha de Inventario,
            #             Observaciones.
            # El join se hace por nombre normalizado + Unidad de Negocio
            # para ser inmune a diferencias de acentos o espacios.
            if not df_ins.empty:
                # Preparar tabla maestra con clave normalizada
                df_ins_m = df_ins.copy()
                df_ins_m["_nom_norm"] = df_ins_m["Nombre del Insumo"].apply(normalizar_nombre)
                df_ins_m["_clave"] = (
                    df_ins_m["Unidad de Negocio"].fillna("") + "||" + df_ins_m["_nom_norm"]
                )
                COLS_ESTATICAS = [
                    "_clave",
                    "Nombre del Insumo",
                    "Marca",
                    "Proveedor",
                    "Grupo",
                    "Presentación de Compra",
                    "Unidad de Medida",
                    "Stock Mínimo",
                    "Tara",
                ]
                df_ins_m = df_ins_m[[c for c in COLS_ESTATICAS if c in df_ins_m.columns]].copy()
                df_ins_m["Stock Mínimo"] = df_ins_m["Stock Mínimo"].apply(limpiar_valor)
                df_ins_m["Tara"] = df_ins_m["Tara"].apply(limpiar_valor) if "Tara" in df_ins_m.columns else 0.0
                df_ins_m = df_ins_m.drop_duplicates(subset=["_clave"], keep="last")

                # Preparar clave en historial
                df_total["_nom_norm"] = df_total["Nombre del Insumo"].apply(normalizar_nombre)
                df_total["_clave"] = (
                    df_total["Unidad de Negocio"].fillna("") + "||" + df_total["_nom_norm"]
                )

                # Columnas de cifras que el historial conserva intactas
                COLS_CIFRAS = [
                    "_clave",
                    "Unidad de Negocio",
                    "Alm",
                    "Barra",
                    "Stock Neto",
                    "¿Comprar?",
                    "Responsable",
                    "Fecha de Inventario",
                    "Fecha de Entrada",
                    "Observaciones",
                ]
                cols_cifras_ok = [c for c in COLS_CIFRAS if c in df_total.columns]
                df_cifras = df_total[cols_cifras_ok].copy()

                # Merge: historial (cifras) LEFT JOIN insumos (estáticos)
                df_total = df_cifras.merge(
                    df_ins_m,
                    on="_clave",
                    how="left",
                )
                df_total.drop(
                    columns=["_clave", "_nom_norm"],
                    inplace=True, errors="ignore"
                )
            # ─────────────────────────────────────────────────────────────────

        return df_ins, df_total

    except Exception as e:
        st.error(f"Falla en extracción de datos: {e}")
        return pd.DataFrame(), pd.DataFrame()


@st.cache_data(ttl=60)
def obtener_usuarios():
    """
    TTL=60s. Crea hoja 'Accesos' con datos iniciales si no existe.
    Retorna (dict_pin→usuario, lista_nombres, df_completo).
    """
    if sh is None:
        return {}, [], pd.DataFrame()

    ws, err = safe_worksheet(sh, "Accesos")
    if err:
        try:
            ws = sh.add_worksheet(title="Accesos", rows="100", cols="3")
            ws.append_row(COLS_ACCESOS)
            ws.append_rows([
                ["13070518", "Raúl",    "admin"],
                ["987654",   "Jenny",   "barista"],
                ["ilecara",  "Araceli", "barista"],
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
            if str(r.get("Clave", "")).strip()
        }
        lista_nombres = df_usr["Nombre"].dropna().tolist()
        return usuarios_dict, lista_nombres, df_usr

    except Exception as e:
        st.warning(f"Error cargando usuarios: {e}")
        return {}, [], pd.DataFrame()


USUARIOS_PIN, LISTA_RESPONSABLES, DF_USUARIOS = obtener_usuarios()


# ============================================================
# LÓGICA DE NEGOCIO
# ============================================================
def obtener_ultimo_inventario(df_hist: pd.DataFrame, unidad: str = None) -> pd.DataFrame:
    """
    Retorna el último registro por insumo (y unidad si se especifica).
    Stock Neto Calculado = Alm + Barra (conteo físico, no teórico).

    CORRECCIÓN B: la deduplicación usa nombre normalizado para evitar
    insumos fantasma por diferencias de espacios o acentos.
    """
    if df_hist.empty:
        return pd.DataFrame()

    df_u = df_hist.copy()
    if unidad:
        df_u = df_u[df_u["Unidad de Negocio"] == unidad]

    if df_u.empty:
        return pd.DataFrame()

    # Fecha efectiva: prioridad a "Fecha de Inventario", fallback a "Fecha de Entrada"
    df_u["_fecha_efectiva"] = df_u["Fecha de Inventario"].combine_first(df_u["Fecha de Entrada"])

    # CORRECCIÓN B: clave de deduplicación con nombre normalizado
    df_u["_nombre_norm"] = df_u["Nombre del Insumo"].apply(normalizar_nombre)

    df_actual = (
        df_u
        .sort_values("_fecha_efectiva", ascending=True, na_position="first")
        .drop_duplicates(subset=["Unidad de Negocio", "_nombre_norm"], keep="last")
        .copy()
    )

    for col in ["Alm", "Barra", "Stock Neto", "Stock Mínimo"]:
        df_actual[col] = df_actual[col].apply(limpiar_valor)

    df_actual["Stock Neto Calculado"] = df_actual["Alm"] + df_actual["Barra"]

    if "¿Comprar?" in df_actual.columns:
        df_actual["Necesita Compra"] = (
            df_actual["¿Comprar?"].astype(str).str.strip().str.upper() == "TRUE"
        )
    else:
        df_actual["Necesita Compra"] = (
            df_actual["Stock Neto Calculado"] < df_actual["Stock Mínimo"]
        )

    df_actual["Fecha de Inventario"] = df_actual["_fecha_efectiva"]
    df_actual.drop(columns=["_fecha_efectiva", "_nombre_norm"], inplace=True, errors="ignore")

    return df_actual


def fecha_max_segura(serie: pd.Series) -> str:
    """Retorna el máximo de timestamps como string en hora Hermosillo. NaT-safe."""
    validas = serie.dropna()
    if validas.empty:
        return "Sin registros"
    return fmt_fecha_hmo(validas.max())


def buscar_insumo_en_actual(df_actual: pd.DataFrame, nombre: str) -> pd.Series:
    """
    Busca un insumo en el inventario actual usando nombre normalizado.
    Retorna la fila como Series, o None si no existe.
    """
    if df_actual.empty:
        return None
    nom_norm = normalizar_nombre(nombre)
    # Comparar con nombres normalizados del inventario actual
    mascaras = df_actual["Nombre del Insumo"].apply(normalizar_nombre) == nom_norm
    if not mascaras.any():
        return None
    return df_actual[mascaras].iloc[0]


# ============================================================
# ESTADO DE SESIÓN — inicialización única y segura
# ============================================================
_defaults = {
    "auth_status": False,
    "current_user": None,
    "user_role":    None,
    "pagina":       "Dashboard",
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
            submitted = st.form_submit_button(
                "Desbloquear Sistema", type="primary", use_container_width=True
            )
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
            for k in ["auth_status", "current_user", "user_role"]:
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
    if st.button("📦 3. Inventario actual",     use_container_width=True): cambiar_pagina("Consulta")

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
Productos abertos que se contaminan con el tiempo. Revisar nivel y estado del envase diariamente.
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

    # — TABLA DE GRUPOS —
    with st.expander("📊 Tabla Resumen de Grupos"):
        grupos_info = [
            {"Grupo": "A", "Nombre": "Café, Leches y Lácteos",        "Rutina": "Diaria",      "Riesgo": "Alto",  "Almacén": "Refrigerador / Bodega seca", "Nota": "Contar antes de abrir"},
            {"Grupo": "B", "Nombre": "Jarabes, Salsas y Bases",       "Rutina": "Diaria",      "Riesgo": "Alto",  "Almacén": "Repisa barra / Refrigerador","Nota": "Revisar envases abiertos"},
            {"Grupo": "C", "Nombre": "Polvos, Tés y Tisanas",         "Rutina": "Diaria",      "Riesgo": "Medio", "Almacén": "Bodega seca hermética",       "Nota": "Proteger de humedad"},
            {"Grupo": "D", "Nombre": "Empaques y Desechables",        "Rutina": "Cada 2 días", "Riesgo": "Medio", "Almacén": "Bodega empaques",             "Nota": "Contar en piezas/rollos"},
            {"Grupo": "E", "Nombre": "Suministros de Limpieza",       "Rutina": "Cada 2 días", "Riesgo": "Bajo",  "Almacén": "Bodega limpieza",             "Nota": "Separar de alimentos"},
            {"Grupo": "F", "Nombre": "Comida y Vitrina",              "Rutina": "Cada 2 días", "Riesgo": "Alto",  "Almacén": "Vitrina / Refrigerador",      "Nota": "Verificar caducidad"},
            {"Grupo": "G", "Nombre": "Retail, Utensilios y Otros",   "Rutina": "Cada 2 días", "Riesgo": "Bajo",  "Almacén": "Bodega general / Mostrador",  "Nota": "Registrar cada salida"},
        ]
        st.dataframe(
            pd.DataFrame(grupos_info),
            hide_index=True, use_container_width=True
        )

    # --- ZONA ADMIN ---
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
            n_rol    = st.selectbox("Nivel de Permisos:", ["barista", "admin"])

            if st.button("➕ Guardar Usuario", use_container_width=True):
                if n_nombre and n_clave:
                    ws_acc, err = safe_worksheet(sh, "Accesos")
                    if err:
                        st.error(err)
                    else:
                        try:
                            nuevo_df  = DF_USUARIOS.copy()
                            nuevo_df  = nuevo_df[nuevo_df["Nombre"] != n_nombre]
                            nueva_fil = pd.DataFrame([{
                                "Clave": str(n_clave), "Nombre": n_nombre, "Rol": n_rol
                            }])
                            nuevo_df = pd.concat([nuevo_df, nueva_fil], ignore_index=True)
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

    # --- CATÁLOGO ---
    if st.session_state.auth_status:
        # CORRECCIÓN E: cargar datos frescos en cada sección que los necesite
        df_raw_sb, _ = cargar_datos_integrales()

        st.divider()
        st.subheader("🛠️ Gestión del Catálogo")
        op_cat = st.radio("Acción:", ["Añadir Insumo", "Editar Insumo"])

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
                tara_new = st.number_input("Tara (kg/gr)", min_value=0.0, value=0.0,
                                           help="Peso del contenedor vacío para este insumo")

                if st.form_submit_button("✨ Crear Insumo"):
                    if not n.strip():
                        st.error("El nombre del insumo es obligatorio.")
                    else:
                        ws_ins, err = safe_worksheet(sh, "Insumos")
                        if err:
                            st.error(err)
                        else:
                            try:
                                nueva_fila = [u, n.strip(), m, p, g, "", uc, um, "", "", "", sm, tara_new]
                                ws_ins.append_row(nueva_fila, value_input_option="USER_ENTERED")
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
                    ins_edit = st.selectbox(
                        "Seleccionar Insumo a Editar:", sorted(ins_nombres)
                    )
                    mask = df_raw_sb["Nombre del Insumo"] == ins_edit
                    if not mask.any():
                        st.warning("Insumo no encontrado en el catálogo actual.")
                    else:
                        d = df_raw_sb[mask].iloc[0]

                        with st.form("f_edit"):
                            unidad_val = d.get("Unidad de Negocio", UNIDADES[0])
                            e_u = st.selectbox(
                                "Unidad", UNIDADES,
                                index=UNIDADES.index(unidad_val) if unidad_val in UNIDADES else 0
                            )
                            e_n  = st.text_input("Nombre",  value=str(d.get("Nombre del Insumo", "")))
                            e_m  = st.text_input("Marca",   value=str(d.get("Marca", "")))
                            e_p  = st.text_input("Proveedor", value=str(d.get("Proveedor", "")))
                            grupo_val = str(d.get("Grupo", "A"))
                            e_g  = st.selectbox(
                                "Grupo", GRUPOS,
                                index=GRUPOS.index(grupo_val) if grupo_val in GRUPOS else 0
                            )
                            e_uc = st.text_input(
                                "Presentación Compra",
                                value=str(d.get("Presentación de Compra", ""))
                            )
                            u_val = str(d.get("Unidad de Medida", "pz")).lower()
                            e_um = st.selectbox(
                                "Medida", UNIDADES_MED,
                                index=UNIDADES_MED.index(u_val) if u_val in UNIDADES_MED else 0
                            )
                            e_sm = st.number_input(
                                "Stock Mínimo", min_value=0.0,
                                value=limpiar_valor(d.get("Stock Mínimo", 0))
                            )
                            e_tara = st.number_input(
                                "Tara (kg/gr)", min_value=0.0,
                                value=limpiar_valor(d.get("Tara", 0)),
                                help="Peso del contenedor vacío. Se aplica automáticamente en cada inventario."
                            )

                            if st.form_submit_button("💾 Actualizar Insumo"):
                                if not e_n.strip():
                                    st.error("El nombre no puede quedar vacío.")
                                else:
                                    ws_ins, err = safe_worksheet(sh, "Insumos")
                                    if err:
                                        st.error(err)
                                    else:
                                        try:
                                            idx = int(d.get("Sheet_Row_Num", 0))
                                            if idx < 2:
                                                raise ValueError("Número de fila inválido.")
                                            fila_act = [[
                                                e_u, e_n.strip(), e_m, e_p, e_g,
                                                "", e_uc, e_um, "", "", "", e_sm, e_tara
                                            ]]
                                            ws_ins.update(
                                                range_name=f"A{idx}:M{idx}", values=fila_act
                                            )
                                            st.cache_data.clear()
                                            st.success("Catálogo actualizado.")
                                            time.sleep(1)
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Error al actualizar: {e}")


# ============================================================
# PÁGINAS PRINCIPALES
# CORRECCIÓN E aplicada en cada página:
#   df_raw, df_historial = cargar_datos_integrales()
# El caché de 30s absorbe llamadas repetidas.
# Después de cache_data.clear() + rerun(), los datos
# se recargan correctamente en lugar de usar la copia de módulo.
# ============================================================
pagina = st.session_state.pagina


# ------ DASHBOARD ------
if pagina == "Dashboard":
    df_raw, df_historial = cargar_datos_integrales()  # CORRECCIÓN E
    st.title("📊 Dashboard Operativo")

    ahora = ahora_hermosillo()
    dias_faltantes = calendar.monthrange(ahora.year, ahora.month)[1] - ahora.day
    if dias_faltantes <= 4:
        st.info(
            f"⏳ A {dias_faltantes} días del fin de mes. "
            "Recuerda ejecutar el **Corte de Mes**."
        )

    df_actual = obtener_ultimo_inventario(df_historial)

    if not df_actual.empty:
        # "Necesita Compra" viene del campo marcado manualmente por el barista (🛒).
        # obtener_ultimo_inventario ya lo parsea como bool desde el Sheet.
        # NO recalcular desde stock vs mínimo — eso pisa la decisión del operador.
        crit = df_actual[df_actual["Necesita Compra"] == True]

        c1, c2, c3 = st.columns(3)
        c1.metric("🛒 Pendientes Noble",
                  len(crit[crit["Unidad de Negocio"] == "Noble"]))
        c2.metric("🛒 Pendientes Coffee Station",
                  len(crit[crit["Unidad de Negocio"] == "Coffee Station"]))
        c3.metric("🕒 Último Movimiento",
                  fecha_max_segura(df_actual["Fecha de Inventario"]))

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🏢 Faltantes: Noble")
            ins_n = crit[crit["Unidad de Negocio"] == "Noble"]
            if not ins_n.empty:
                for _, r in ins_n.iterrows():
                    fecha_str = fmt_fecha_hmo(r.get("Fecha de Inventario"))
                    st.error(
                        f"**{r['Nombre del Insumo']}** "
                        f"(Stock: {r['Stock Neto Calculado']} / Mín: {r['Stock Mínimo']})"
                        + (f"  \n🕒 Último conteo: {fecha_str}" if fecha_str else "")
                    )
            else:
                st.success("Operación cubierta.")
        with col2:
            st.subheader("☕ Faltantes: Coffee Station")
            ins_cs = crit[crit["Unidad de Negocio"] == "Coffee Station"]
            if not ins_cs.empty:
                for _, r in ins_cs.iterrows():
                    fecha_str = fmt_fecha_hmo(r.get("Fecha de Inventario"))
                    st.error(
                        f"**{r['Nombre del Insumo']}** "
                        f"(Stock: {r['Stock Neto Calculado']} / Mín: {r['Stock Mínimo']})"
                        + (f"  \n🕒 Último conteo: {fecha_str}" if fecha_str else "")
                    )
            else:
                st.success("Operación cubierta.")

        st.divider()
        st.subheader("🕒 Actividad Reciente")
        df_log = df_historial.copy()
        df_log["Fecha de Inventario"] = df_log["Fecha de Inventario"].combine_first(
            df_log["Fecha de Entrada"]
        )
        cols_log = [
            "Fecha de Inventario", "Responsable", "Unidad de Negocio",
            "Nombre del Insumo", "Stock Neto", "¿Comprar?", "Observaciones"
        ]
        cols_log_ok = [c for c in cols_log if c in df_log.columns]
        st.dataframe(
            df_log.dropna(subset=["Fecha de Inventario"])
                  .sort_values("Fecha de Inventario", ascending=False)[cols_log_ok]
                  .head(15),
            use_container_width=True
        )
    else:
        st.info("Sin datos históricos. Ejecuta el primer conteo de inventario.")


# ------ CAPTURAR INVENTARIO ------
elif pagina == "Inventario":
    df_raw, df_historial = cargar_datos_integrales()  # CORRECCIÓN E
    st.title("📝 Capturar inventario")

    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida.")
        st.stop()

    col_u, col_r, col_g = st.columns([1, 1, 2])
    with col_u:
        u_sel = st.selectbox("🏢 Unidad de Negocio", UNIDADES)

    responsables = st.session_state.responsables or ["Raúl"]
    resp_idx = (
        responsables.index(st.session_state.current_user)
        if st.session_state.current_user in responsables else 0
    )
    with col_r:
        r_sel = st.selectbox(
            "👤 Responsable", responsables, index=resp_idx,
            disabled=(st.session_state.user_role != "admin")
        )

    df_u = (
        df_raw[df_raw["Unidad de Negocio"] == u_sel]
        if not df_raw.empty else pd.DataFrame()
    )

    with col_g:
        grps = (
            sorted(df_u["Grupo"].dropna().unique().tolist())
            if not df_u.empty and "Grupo" in df_u.columns else GRUPOS
        )
        g_sel = st.multiselect("📂 Grupos a contar", grps, default=grps[:1] if grps else [])

    st.divider()
    busqueda_inv = st.text_input("🔍 Buscar insumo:", placeholder="Escribe el nombre...")

    df_actual = obtener_ultimo_inventario(df_historial, u_sel)

    df_f = (
        df_u[df_u["Grupo"].isin(g_sel)]
        .sort_values(["Grupo", "Nombre del Insumo"])
        .reset_index(drop=True)
        if not df_u.empty and g_sel else pd.DataFrame()
    )
    if busqueda_inv and not df_f.empty:
        df_f = df_f[
            df_f["Nombre del Insumo"].astype(str)
            .str.contains(busqueda_inv, case=False, na=False)
        ]

    if df_f.empty:
        st.info("Selecciona al menos un grupo para mostrar insumos.")
    else:
        # ── CORRECCIÓN BUFFERING: envolver todo en st.form para evitar reruns
        # en cada keystroke. Los valores solo se procesan al presionar submit.
        # Esto elimina el "buffering" por rerun en cada input numérico.
        with st.form("form_inventario", clear_on_submit=False):
            # Encabezados
            h1, h2, h3, h4, h5, h6, h7 = st.columns([2.8, 1.0, 1.0, 1.0, 1.0, 1.2, 2.5])
            for col, label in zip(
                [h1, h2, h3, h4, h5, h6, h7],
                ["Insumo / Ref", "Almacén", "Barra", "Medida", "Neto*", "¿Pedir?", "Observaciones"]
            ):
                col.write(f"**{label}**")
            st.markdown("*Neto = Alm + Barra − Tara guardada en catálogo")
            st.divider()

            regs_form = {}
            for _, row in df_f.iterrows():
                nom      = str(row.get("Nombre del Insumo", ""))
                safe_nom = re.sub(r'[^a-zA-Z0-9]', '_', nom)[:40]

                # CORRECCIÓN B: buscar con nombre normalizado
                prev = buscar_insumo_en_actual(df_actual, nom)
                v_prev = prev["Stock Neto Calculado"] if prev is not None else 0.0
                v_min  = limpiar_valor(row.get("Stock Mínimo", 0))
                # Tara guardada en el catálogo de Insumos
                v_tara_cat = limpiar_valor(row.get("Tara", 0))

                c1, c2, c3, c4, c5, c6, c7 = st.columns([2.8, 1.0, 1.0, 1.0, 1.0, 1.2, 2.5])
                with c1:
                    st.write(f"**{nom}**")
                    st.caption(f"Marca: {row.get('Marca','-')} | Prov: {row.get('Proveedor','-')}")
                    diff  = v_prev - v_min
                    color = "green" if diff >= 0 else "red"
                    tara_txt = f" | Tara: {v_tara_cat}" if v_tara_cat > 0 else ""
                    st.markdown(
                        f"<small>Anterior: {v_prev} | Mín: {v_min} "
                        f"(<span style='color:{color}'>{diff:+.1f}</span>){tara_txt}</small>",
                        unsafe_allow_html=True
                    )
                with c2:
                    v_a = st.number_input(
                        "Alm", min_value=0.0, step=1.0, value=0.0,
                        key=f"a_{safe_nom}", label_visibility="collapsed",
                    )
                with c3:
                    v_b = st.number_input(
                        "Bar", min_value=0.0, step=1.0, value=0.0,
                        key=f"b_{safe_nom}", label_visibility="collapsed",
                    )
                with c4:
                    u_act = str(row.get("Unidad de Medida", "pz")).lower()
                    v_u = st.selectbox(
                        "U", UNIDADES_MED,
                        index=UNIDADES_MED.index(u_act) if u_act in UNIDADES_MED else 0,
                        key=f"u_{safe_nom}", label_visibility="collapsed"
                    )
                with c5:
                    # Neto calculado usando tara del catálogo (no hay interacción, se muestra como texto)
                    v_n_display = max(0.0, (v_a + v_b) - v_tara_cat)
                    st.write(f"**{v_n_display:.1f}**")
                with c6:
                    v_p = st.toggle("🛒", key=f"p_{safe_nom}", value=False)
                with c7:
                    v_c = st.text_input(
                        "Nota...", key=f"c_{safe_nom}",
                        label_visibility="collapsed", placeholder="Opcional"
                    )

                regs_form[nom] = {
                    "a": v_a, "b": v_b, "n": v_n_display,
                    "u": v_u, "p": v_p, "c": v_c, "row": row
                }
                st.divider()

            btn_inv = st.form_submit_button(
                "📥 PROCESAR INVENTARIO",
                use_container_width=True,
                type="primary",
            )

        # Procesar fuera del form para evitar doble submit
        if btn_inv:
            ws_his, err = safe_worksheet(sh, "Historial")
            if err:
                st.error(err)
            else:
                fh    = ts_hermosillo()
                filas = []
                for n, info in regs_form.items():
                    dm = info["row"]
                    filas.append([
                        u_sel, n,
                        dm.get("Marca", ""),    dm.get("Proveedor", ""),
                        dm.get("Grupo", ""),    "",
                        dm.get("Presentación de Compra", ""), info["u"],
                        info["a"], info["b"],   info["n"],
                        dm.get("Stock Mínimo", 0),
                        "TRUE" if info["p"] else "FALSE",
                        r_sel, fh, info["c"]
                    ])
                ok, msg = append_rows_con_retry(ws_his, filas)
                if ok:
                    st.cache_data.clear()
                    st.success(f"¡Inventario registrado! {msg}")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error(msg)


# ------ ENTRADA DE COMPRAS ------
elif pagina == "Ingresos":
    df_raw, df_historial = cargar_datos_integrales()  # CORRECCIÓN E
    st.title("📥 Entrada de compras")

    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida.")
        st.stop()

    st.info("Ingresa insumos recibidos. Se sumarán al último stock de Almacén registrado.")

    col_u, col_r = st.columns(2)
    with col_u:
        u_sel = st.selectbox("🏢 Unidad receptora:", UNIDADES)

    responsables = st.session_state.responsables or ["Raúl"]
    resp_idx = (
        responsables.index(st.session_state.current_user)
        if st.session_state.current_user in responsables else 0
    )
    with col_r:
        r_sel = st.selectbox(
            "👤 Responsable:", responsables, index=resp_idx,
            disabled=(st.session_state.user_role != "admin")
        )

    df_u = (
        df_raw[df_raw["Unidad de Negocio"] == u_sel]
        if not df_raw.empty else pd.DataFrame()
    )
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
            # CORRECCIÓN B: buscar con nombre normalizado
            prev = buscar_insumo_en_actual(df_actual, nom)
            v_a_prev = prev["Alm"]   if prev is not None else 0.0
            v_b_prev = prev["Barra"] if prev is not None else 0.0
            bulk_data.append({
                "Insumo": nom, "Stock Alm": v_a_prev,
                "Stock Barra": v_b_prev, "+ Ingreso": 0.0
            })

        df_edit   = pd.DataFrame(bulk_data)
        edited_df = st.data_editor(
            df_edit[["Insumo", "Stock Alm", "Stock Barra", "+ Ingreso"]],
            hide_index=True, use_container_width=True,
            disabled=["Insumo", "Stock Alm", "Stock Barra"]
        )

        # CORRECCIÓN C: bloqueo de doble envío
        proc_bulk = st.session_state.get("_procesando_bulk", False)
        btn_bulk  = st.button(
            "📦 EJECUTAR INGRESO BULK", type="primary", disabled=proc_bulk
        )

        if btn_bulk and not proc_bulk:
            st.session_state["_procesando_bulk"] = True
            ws_his, err = safe_worksheet(sh, "Historial")
            if err:
                st.error(err)
                st.session_state["_procesando_bulk"] = False
            else:
                fh         = ts_hermosillo()
                filas_bulk = []
                for _, r_ed in edited_df.iterrows():
                    ingreso = limpiar_valor(r_ed["+ Ingreso"])
                    if ingreso <= 0:
                        continue
                    nom  = r_ed["Insumo"]
                    orig = next((x for x in bulk_data if x["Insumo"] == nom), None)
                    if orig is None:
                        continue
                    row_matches = df_u[df_u["Nombre del Insumo"] == nom]
                    if row_matches.empty:
                        continue
                    row_ins = row_matches.iloc[0]
                    v_min   = limpiar_valor(row_ins.get("Stock Mínimo", 0))
                    nuevo_a = orig["Stock Alm"] + ingreso
                    nuevo_n = nuevo_a + orig["Stock Barra"]
                    filas_bulk.append([
                        u_sel, nom,
                        row_ins.get("Marca", ""),    row_ins.get("Proveedor", ""),
                        row_ins.get("Grupo", ""),    fh,
                        row_ins.get("Presentación de Compra", ""),
                        row_ins.get("Unidad de Medida", "pz"),
                        nuevo_a, orig["Stock Barra"], nuevo_n, v_min,
                        "TRUE" if nuevo_n < v_min else "FALSE", r_sel, "", ""
                    ])

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
            h1, h2, h3, h4, h5 = st.columns([3, 2, 1.5, 1.5, 2])
            for col, label in zip(
                [h1, h2, h3, h4, h5],
                ["Insumo", "Stock Ant (Alm+Bar)", "+ Cantidad", "Tara", "= Nuevo Total"]
            ):
                col.write(f"**{label}**")
            st.divider()

            for i, nom in enumerate(insumos_llegados):
                row_matches = df_u[df_u["Nombre del Insumo"] == nom]
                if row_matches.empty:
                    continue
                row_ins = row_matches.iloc[0]
                # CORRECCIÓN B: buscar con nombre normalizado
                prev     = buscar_insumo_en_actual(df_actual, nom)
                v_a_prev = prev["Alm"]   if prev is not None else 0.0
                v_b_prev = prev["Barra"] if prev is not None else 0.0
                v_min    = limpiar_valor(row_ins.get("Stock Mínimo", 0))

                c1, c2, c3, c4, c5 = st.columns([3, 2, 1.5, 1.5, 2])
                with c1:
                    st.write(f"**{nom}**")
                    st.caption(
                        f"Marca: {row_ins.get('Marca','-')} | "
                        f"Prov: {row_ins.get('Proveedor','-')}"
                    )
                with c2:
                    st.write(f"Almacén: {v_a_prev} | Barra: {v_b_prev}")
                    st.write(f"**Total Ant: {v_a_prev + v_b_prev}**")
                with c3:
                    cant_ingreso = st.number_input(
                        "Ingreso", min_value=0.0, step=1.0, value=None,
                        key=f"ing_{i}", label_visibility="collapsed",
                        placeholder="0"
                    )
                    cant_ingreso = cant_ingreso if cant_ingreso is not None else 0.0
                with c4:
                    tara_ingreso = st.number_input(
                        "Tara", min_value=0.0, step=0.1, value=None,
                        key=f"tara_ing_{i}", label_visibility="collapsed",
                        placeholder="tara",
                        help="Peso del contenedor (tara). Se restará del ingreso capturado."
                    )
                    tara_ingreso = tara_ingreso if tara_ingreso is not None else 0.0
                with c5:
                    cant_neta  = max(0.0, cant_ingreso - tara_ingreso)
                    nuevo_alm  = v_a_prev + cant_neta
                    nuevo_neto = nuevo_alm + v_b_prev
                    st.success(f"**{nuevo_neto:.1f}**")

                regs_ingreso[nom] = {
                    "nuevo_a": nuevo_alm, "b": v_b_prev,
                    "nuevo_n": nuevo_neto, "row": row_ins, "min": v_min
                }
                st.divider()

            # CORRECCIÓN C: bloqueo de doble envío
            proc_ing = st.session_state.get("_procesando_ingreso", False)
            btn_ing  = st.button(
                "📦 EJECUTAR INGRESO", use_container_width=True,
                type="primary", disabled=proc_ing
            )

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
                        filas.append([
                            u_sel, n,
                            dm.get("Marca", ""),   dm.get("Proveedor", ""),
                            dm.get("Grupo", ""),   fh,
                            dm.get("Presentación de Compra", ""),
                            dm.get("Unidad de Medida", "pz"),
                            info["nuevo_a"], info["b"], info["nuevo_n"],
                            info["min"],
                            "TRUE" if info["nuevo_n"] < info["min"] else "FALSE",
                            r_sel, "", ""
                        ])
                    ok, msg = append_rows_con_retry(ws_his, filas)
                    st.session_state["_procesando_ingreso"] = False
                    if ok:
                        st.cache_data.clear()
                        st.success(f"Ingreso registrado. {msg}")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)


# ------ INVENTARIO ACTUAL ------
elif pagina == "Consulta":
    df_raw, df_historial = cargar_datos_integrales()  # CORRECCIÓN E
    st.title("📦 Inventario actual")
    u_sel     = st.selectbox("🏢 Unidad:", UNIDADES)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)

    if df_actual.empty:
        st.warning("No hay registros en la base de datos para esta unidad.")
        st.stop()

    bajo_min = df_actual[df_actual["Necesita Compra"] == True]
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Referencias", len(df_actual))
    m2.metric("Alertas de Compra", len(bajo_min), delta=-len(bajo_min), delta_color="inverse")
    m3.metric("Volumen Global", f"{df_actual['Stock Neto Calculado'].sum():,.1f}")

    st.divider()
    col_s, col_p = st.columns([2, 1])
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
        df_display = df_display[
            df_display["Nombre del Insumo"].astype(str)
            .str.contains(busqueda, case=False, na=False)
        ]
    if prov_sel != "Todos" and col_prov:
        df_display = df_display[df_display[col_prov] == prov_sel]

    col_map = {
        "Grupo":             "Grupo",
        "Nombre del Insumo":   "Insumo",
        "Marca":             "Marca",
        "Proveedor":         "Proveedor",
        "Alm":                 "Almacén",
        "Barra":               "Barra",
        "Stock Neto Calculado":"Stock Total",
        "Unidad de Medida":     "Medida",
        "Stock Mínimo":        "Mínimo",
        "Necesita Compra":     "¿Comprar?",
        "Responsable":         "Responsable",
        "Fecha de Inventario": "Último Corte",
        "Observaciones":         "Observaciones",
    }
    cols_ok  = [c for c in col_map if c in df_display.columns]
    df_final = df_display[cols_ok].rename(columns=col_map)

    def highlight_low(row):
        total  = row.get("Stock Total", 9999)
        minimo = row.get("Mínimo", 0)
        color  = "background-color: rgba(255, 75, 75, 0.2)" if total < minimo else ""
        return [color] * len(row)

    st.dataframe(
        df_final.style.apply(highlight_low, axis=1),
        use_container_width=True, hide_index=True
    )

    st.divider()
    csv = df_final.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Descargar Reporte (CSV)", data=csv,
        file_name=f"Inventario_{u_sel}_{ahora_hermosillo().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv", use_container_width=True
    )


# ------ LISTA DE CONTEO (PDF 58mm) ------
elif pagina == "Impresion":
    df_raw, _ = cargar_datos_integrales()  # CORRECCIÓN E
    st.title("🖨️ Ticket de Conteo (58mm)")
    u_sel = st.selectbox("Sucursal:", UNIDADES)
    df_u  = (
        df_raw[df_raw["Unidad de Negocio"] == u_sel]
        if not df_raw.empty else pd.DataFrame()
    )
    grps  = (
        sorted(df_u["Grupo"].dropna().unique().tolist())
        if not df_u.empty and "Grupo" in df_u.columns else []
    )
    g_sel = st.multiselect("Filtrar por Grupos:", grps)

    if g_sel and not df_u.empty:
        df_p  = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"])

        # Construir líneas para PDF
        lineas_pdf = []
        lineas_pdf.append((f"* CONTEO {u_sel.upper()} *", "title"))
        lineas_pdf.append((f"Fecha: {ahora_hermosillo().strftime('%d/%m/%Y')}", "small"))
        lineas_pdf.append(("", "divider"))

        gr_actual = ""
        for _, r in df_p.iterrows():
            grupo = str(r.get("Grupo", ""))
            if grupo != gr_actual:
                lineas_pdf.append((f">> GRUPO {grupo} <<", "bold"))
                gr_actual = grupo
            nom = str(r['Nombre del Insumo'])[:22]
            lineas_pdf.append((nom, "normal"))
            lineas_pdf.append(("[    ] Alm   [    ] Bar", "small"))
            lineas_pdf.append(("", "divider"))

        # Previsualización de texto
        with st.expander("👁️ Vista previa del contenido", expanded=True):
            prev_txt = f"{'='*28}\n* CONTEO {u_sel.upper()} *\nFecha: {ahora_hermosillo().strftime('%d/%m/%Y')}\n{'-'*28}\n"
            gr_actual_p = ""
            for _, r in df_p.iterrows():
                grupo = str(r.get("Grupo", ""))
                if grupo != gr_actual_p:
                    prev_txt += f"\n>> GRUPO {grupo} <<\n"
                    gr_actual_p = grupo
                prev_txt += f" {str(r['Nombre del Insumo'])[:22]}\n"
                prev_txt += " [    ] Alm   [    ] Bar\n"
                prev_txt += "-" * 28 + "\n"
            st.code(prev_txt, language=None)

        # Generar PDF y botón de descarga
        pdf_bytes = generar_pdf_58mm(f"Conteo {u_sel}", lineas_pdf)
        nombre_archivo = f"conteo_{u_sel.replace(' ','_')}_{ahora_hermosillo().strftime('%Y%m%d_%H%M')}.pdf"
        st.download_button(
            label="📄 Descargar PDF 58mm",
            data=pdf_bytes,
            file_name=nombre_archivo,
            mime="application/pdf",
            use_container_width=True,
            type="primary"
        )
    else:
        st.info("Selecciona grupos para generar la lista.")


# ------ LISTA DE COMPRA (PDF 58mm) ------
elif pagina == "ListaCompra":
    _, df_historial = cargar_datos_integrales()  # CORRECCIÓN E
    st.title("🛒 Ticket de Compra (58mm)")
    u_sel     = st.radio("Generar orden para:", UNIDADES, horizontal=True)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)

    if df_actual.empty:
        st.info("Sin registros para armar la lista de compra.")
        st.stop()

    com = df_actual[df_actual["Necesita Compra"] == True]
    if not com.empty:
        # Construir líneas para PDF
        lineas_pdf = []
        lineas_pdf.append((f"* COMPRAS {u_sel.upper()} *", "title"))
        lineas_pdf.append((f"Fecha: {ahora_hermosillo().strftime('%d/%m/%Y')}", "small"))
        lineas_pdf.append(("", "divider"))

        for _, r in com.iterrows():
            nom = str(r['Nombre del Insumo'])[:22]
            lineas_pdf.append((f"* {nom}", "bold"))
            lineas_pdf.append((f"  Stock:{r['Stock Neto Calculado']} Min:{r['Stock Mínimo']}", "small"))
            lineas_pdf.append(("", "divider"))

        # Previsualización
        with st.expander("👁️ Vista previa del contenido", expanded=True):
            prev_txt = f"{'='*28}\n* COMPRAS {u_sel.upper()} *\nFecha: {ahora_hermosillo().strftime('%d/%m/%Y')}\n{'-'*28}\n"
            for _, r in com.iterrows():
                prev_txt += f"• {str(r['Nombre del Insumo'])[:22]}\n"
                prev_txt += f"  Stock: {r['Stock Neto Calculado']} / Min: {r['Stock Mínimo']}\n"
                prev_txt += "-" * 28 + "\n"
            st.code(prev_txt, language=None)

        # PDF y descarga
        pdf_bytes = generar_pdf_58mm(f"Compras {u_sel}", lineas_pdf)
        nombre_archivo = f"compras_{u_sel.replace(' ','_')}_{ahora_hermosillo().strftime('%Y%m%d_%H%M')}.pdf"
        st.download_button(
            label="📄 Descargar PDF 58mm",
            data=pdf_bytes,
            file_name=nombre_archivo,
            mime="application/pdf",
            use_container_width=True,
            type="primary"
        )
    else:
        st.success("No hay alertas de reabastecimiento activas.")


# ------ REPORTE DE STOCK (PDF 58mm) ------
elif pagina == "ReporteStock":
    _, df_historial = cargar_datos_integrales()  # CORRECCIÓN E
    st.title("📦 Reporte de Stock (58mm)")
    u_sel     = st.radio("Generar reporte para:", UNIDADES, horizontal=True)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)

    if df_actual.empty:
        st.warning("Sin registros para generar el reporte.")
        st.stop()

    col_grupo = "Grupo" if "Grupo" in df_actual.columns else "Grupo"
    df_rep    = df_actual.sort_values([col_grupo, "Nombre del Insumo"])

    # Construir líneas para PDF
    lineas_pdf = []
    lineas_pdf.append((f"* INVENTARIO {u_sel.upper()} *", "title"))
    lineas_pdf.append((ahora_hermosillo().strftime('%d/%m/%Y %H:%M'), "small"))
    lineas_pdf.append(("", "divider"))

    gr_actual = ""
    for _, r in df_rep.iterrows():
        grupo = str(r.get(col_grupo, ""))
        if grupo != gr_actual:
            lineas_pdf.append((f">> GRUPO {grupo} <<", "bold"))
            gr_actual = grupo
        nom = str(r['Nombre del Insumo'])[:20]
        lineas_pdf.append((nom, "normal"))
        lineas_pdf.append((f" Alm:{r['Alm']} Bar:{r['Barra']} Tot:{r['Stock Neto Calculado']}", "small"))

    lineas_pdf.append(("", "divider"))

    # Previsualización
    with st.expander("👁️ Vista previa del contenido", expanded=True):
        prev_txt = f"{'='*28}\n* INVENTARIO {u_sel.upper()} *\n{ahora_hermosillo().strftime('%d/%m/%Y %H:%M')}\n{'-'*28}\n"
        gr_actual_p = ""
        for _, r in df_rep.iterrows():
            grupo = str(r.get(col_grupo, ""))
            if grupo != gr_actual_p:
                prev_txt += f"\n>> GRUPO {grupo} <<\n"
                gr_actual_p = grupo
            prev_txt += f"{str(r['Nombre del Insumo'])[:20]}\n"
            prev_txt += f" Alm:{r['Alm']} Bar:{r['Barra']} Total:{r['Stock Neto Calculado']}\n"
        prev_txt += "-" * 28 + "\n"
        st.code(prev_txt, language=None)

    # PDF y descarga
    pdf_bytes = generar_pdf_58mm(f"Stock {u_sel}", lineas_pdf)
    nombre_archivo = f"stock_{u_sel.replace(' ','_')}_{ahora_hermosillo().strftime('%Y%m%d_%H%M')}.pdf"
    st.download_button(
        label="📄 Descargar PDF 58mm",
        data=pdf_bytes,
        file_name=nombre_archivo,
        mime="application/pdf",
        use_container_width=True,
        type="primary"
    )


# ------ CORTE DE MES ------
elif pagina == "CorteMes":
    _, df_historial = cargar_datos_integrales()  # CORRECCIÓN E

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
                # PASO 1 — Calcular estados finales
                st.write("1/4 — Calculando estados finales de stock...")
                df_corte = obtener_ultimo_inventario(df_historial)
                if df_corte.empty:
                    st.error("No hay datos de inventario para cerrar.")
                    st.stop()

                fh          = ts_hermosillo()
                encabezados = COLS_HISTORIAL
                col_grupo   = "Grupo"    if "Grupo"    in df_corte.columns else "Grupo"
                col_prov    = "Proveedor" if "Proveedor" in df_corte.columns else "Proveedor"

                filas_corte = []
                for _, r in df_corte.iterrows():
                    filas_corte.append([
                        r.get("Unidad de Negocio", ""), r.get("Nombre del Insumo", ""),
                        r.get("Marca", ""),            r.get(col_prov, ""),
                        r.get(col_grupo, ""),            "",
                        r.get("Presentación de Compra", ""),      r.get("Unidad de Medida", ""),
                        r.get("Alm", 0),                 r.get("Barra", 0),
                        r.get("Stock Neto Calculado", 0), r.get("Stock Mínimo", 0),
                        "TRUE" if r.get("Necesita Compra", False) else "FALSE",
                        "SISTEMA-CIERRE", fh, "Corte consolidado"
                    ])

                # PASO 2 — Archivar historial ANTES de limpiar (protege datos)
                st.write("2/4 — Archivando historial previo...")
                ws_his, err = safe_worksheet(sh, "Historial")
                if err:
                    raise RuntimeError(err)

                datos_hist = ws_his.get_all_values()

                # CORRECCIÓN D: detectar si el historial ya fue limpiado en intento previo
                if len(datos_hist) <= 1:
                    st.warning(
                        "El Historial ya está vacío. Posiblemente el cierre anterior "
                        "completó el paso 4. Verifica que 'Cierres' tenga los datos "
                        "correctos antes de continuar."
                    )
                    status.update(label="⚠️ Historial ya estaba vacío", state="error")
                    st.stop()

                # Obtener o crear Archivo_Historial
                ws_arc, _ = safe_worksheet(sh, "Archivo_Historial")
                if ws_arc is None:
                    ws_arc = sh.add_worksheet(title="Archivo_Historial", rows="10000", cols="20")
                    ws_arc.append_row(encabezados)

                # CORRECCIÓN D: marcar timestamp de archivo para detectar duplicados
                timestamp_marca = [f"=== CORTE {fh} ==="] + [""] * (len(encabezados) - 1)
                ws_arc.append_row(timestamp_marca)
                ws_arc.append_rows(datos_hist[1:])  # Sin encabezado (ya existe en el archivo)

                # PASO 3 — Escribir Cierres con el consolidado
                st.write("3/4 — Consolidando saldos iniciales...")
                ws_cie, _ = safe_worksheet(sh, "Cierres")
                if ws_cie is None:
                    ws_cie = sh.add_worksheet(title="Cierres", rows="1000", cols="20")
                ws_cie.clear()
                ws_cie.append_row(encabezados)
                ws_cie.append_rows(filas_corte)

                # PASO 4 — Limpiar Historial (SIEMPRE el último paso)
                # Si falla aquí, Cierres ya tiene los datos consolidados
                # y Archivo_Historial tiene el backup. No hay pérdida.
                st.write("4/4 — Reiniciando Historial...")
                ws_his.clear()
                ws_his.append_row(encabezados)

                st.cache_data.clear()
                status.update(label="✅ Cierre completado", state="complete")
                st.success(
                    f"Base de datos optimizada. "
                    f"{len(filas_corte)} referencias consolidadas en 'Cierres'. "
                    "Backup completo en 'Archivo_Historial'."
                )
                time.sleep(2)
                st.rerun()

            except Exception as e:
                status.update(label="❌ Falla en el cierre", state="error")
                st.error(
                    f"Error durante el cierre: {e}\n\n"
                    "**El Historial NO fue eliminado.** "
                    "Revisa las pestañas 'Cierres' y 'Archivo_Historial' en el Spreadsheet "
                    "antes de reintentar."
                )
