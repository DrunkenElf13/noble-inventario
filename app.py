import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime
import time
import calendar

# ============================================================
# CONSTANTES CENTRALIZADAS — única fuente de verdad
# Si cambian columnas en el Sheet, solo se edita aquí.
# ============================================================
COLS_INSUMOS = [
    "Unidad de Negocio", "Nombre del Insumo", "Marca", "Proveedor",
    "Grupo", "Espacio_1", "Presentación de Compra", "Unidad de Medida",
    "Espacio_2", "Espacio_3", "Espacio_4", "Stock Mínimo"
]
COLS_HISTORIAL = [
    "Unidad de Negocio", "Nombre del Insumo", "Marca_H", "Proveedor_H",
    "Grupo_H", "Espacio_H", "Pres_Compra_H", "Unidad_Medida_H",
    "Alm", "Barra", "Stock Neto", "Stock Mínimo",
    "Necesita Compra", "Responsable", "Fecha de Inventario", "Comentarios"
]
COLS_ACCESOS   = ["Clave", "Nombre", "Rol"]
GRUPOS         = ["A", "B", "C", "D", "E", "F", "G"]
UNIDADES       = ["Noble", "Coffee Station"]
UNIDADES_MED   = ["pz", "ml", "gr", "kg", "lt"]
SPREADSHEET_ID = "1VZV81p-JqoaRPzMzsRurF6wntVefyaN5ozs3RJe6uJs"

# ============================================================
# HELPERS UTILITARIOS
# ============================================================
def limpiar_valor(valor) -> float:
    """Convierte cualquier valor de celda a float de forma segura."""
    if valor is None:
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor) if not pd.isna(valor) else 0.0
    try:
        return float(str(valor).replace('%', '').replace('$', '').replace(',', '').strip())
    except (ValueError, TypeError):
        return 0.0


def normalizar_dataframe(df: pd.DataFrame, columnas_esperadas: list) -> pd.DataFrame:
    """
    Renombra columnas por posición y garantiza que existan todas las esperadas.
    No elimina columnas extra; las preserva con su nombre original.
    """
    if df.empty:
        return pd.DataFrame(columns=columnas_esperadas)

    cols_actuales = list(df.columns)
    nuevos_nombres = []
    for i, col in enumerate(cols_actuales):
        nuevos_nombres.append(columnas_esperadas[i] if i < len(columnas_esperadas) else col)
    df = df.copy()
    df.columns = nuevos_nombres

    for col in columnas_esperadas:
        if col not in df.columns:
            df[col] = None
    return df


def safe_worksheet(sh, nombre: str):
    """
    Obtiene un worksheet con validación explícita.
    Retorna (worksheet, error_str). Si falla, retorna (None, msg).
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
    Escribe filas con reintentos y backoff exponencial.
    Retorna (éxito: bool, mensaje: str).
    """
    for intento in range(1, max_intentos + 1):
        try:
            worksheet.append_rows(filas, value_input_option="USER_ENTERED")
            return True, f"{len(filas)} fila(s) registrada(s)."
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429 and intento < max_intentos:
                time.sleep(2 ** intento)
                continue
            return False, f"Error API Sheets (intento {intento}): {e}"
        except Exception as e:
            return False, f"Error inesperado al escribir: {e}"
    return False, "Se agotaron los reintentos de escritura."


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
        st.error(f"Error crítico de conexión: {e}")
        return None


sh = conectar_google_sheets()


# ============================================================
# CARGA DE DATOS
# ============================================================
@st.cache_data(ttl=30)
def cargar_datos_integrales():
    """
    Carga Insumos + Historial + Cierres.
    TTL=30s: balance entre frescura y quota de API.
    Retorna (df_insumos, df_historial_total).
    """
    if sh is None:
        return pd.DataFrame(), pd.DataFrame()

    try:
        ws_ins, err = safe_worksheet(sh, "Insumos")
        if err:
            st.warning(err)
            return pd.DataFrame(), pd.DataFrame()

        ws_his, err_his = safe_worksheet(sh, "Historial")
        if err_his:
            st.warning(err_his)
            return pd.DataFrame(), pd.DataFrame()

        val_ins = ws_ins.get_all_values()
        val_his = ws_his.get_all_values()

        # Cierres es opcional
        ws_cie, _ = safe_worksheet(sh, "Cierres")
        val_cie = ws_cie.get_all_values() if ws_cie else []

        def _to_df(vals):
            if len(vals) > 1:
                return pd.DataFrame(vals[1:], columns=vals[0])
            header = vals[0] if vals else []
            return pd.DataFrame(columns=header)

        df_ins = _to_df(val_ins)
        df_his = _to_df(val_his)
        df_cie = _to_df(val_cie) if val_cie else pd.DataFrame()

        # Preservar número de fila para edición posterior
        df_ins["Sheet_Row_Num"] = df_ins.index + 2

        df_ins = normalizar_dataframe(df_ins, COLS_INSUMOS)
        df_his = normalizar_dataframe(df_his, COLS_HISTORIAL)

        if not df_cie.empty:
            df_cie = normalizar_dataframe(df_cie, COLS_HISTORIAL)
            df_total = pd.concat([df_cie, df_his], ignore_index=True)
        else:
            df_total = df_his

        # Parsear fechas de forma robusta
        if not df_total.empty:
            df_total["Fecha de Inventario"] = pd.to_datetime(
                df_total["Fecha de Inventario"], errors="coerce"
            )
            df_total["Espacio_H"] = pd.to_datetime(
                df_total["Espacio_H"], errors="coerce"
            )

        return df_ins, df_total

    except Exception as e:
        st.error(f"Falla en extracción de datos: {e}")
        return pd.DataFrame(), pd.DataFrame()


@st.cache_data(ttl=60)
def obtener_usuarios():
    """
    TTL=60s. Retorna (dict_usuarios, lista_nombres, df_usuarios).
    Crea hoja 'Accesos' con datos demo si no existe.
    """
    if sh is None:
        return {}, [], pd.DataFrame()

    ws, err = safe_worksheet(sh, "Accesos")
    if err:
        try:
            ws = sh.add_worksheet(title="Accesos", rows="100", cols="3")
            ws.append_row(COLS_ACCESOS)
            ws.append_rows([
                ["13070518", "Raúl",   "admin"],
                ["987654",   "Jenny",  "barista"],
                ["ilecara",  "Araceli","barista"],
            ])
        except Exception as e:
            st.warning(f"No se pudo crear hoja Accesos: {e}")
            return {}, [], pd.DataFrame()

    try:
        data = ws.get_all_values()
        if len(data) < 2:
            return {}, [], pd.DataFrame()

        df_usr = pd.DataFrame(data[1:], columns=data[0])
        # Asegurar columnas mínimas
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


df_raw, df_historial = cargar_datos_integrales()
USUARIOS_PIN, LISTA_RESPONSABLES, DF_USUARIOS = obtener_usuarios()


# ============================================================
# LÓGICA DE NEGOCIO
# ============================================================
def obtener_ultimo_inventario(df_hist: pd.DataFrame, unidad: str = None) -> pd.DataFrame:
    """
    Retorna el último registro por insumo (y unidad si se especifica).
    Stock Neto = Alm + Barra (conteo físico real, no resta ventas).
    """
    if df_hist.empty:
        return pd.DataFrame()

    df_u = df_hist.copy()
    if unidad:
        df_u = df_u[df_u["Unidad de Negocio"] == unidad]

    if df_u.empty:
        return pd.DataFrame()

    # Resolver columna de fecha: prioridad a "Fecha de Inventario", fallback a "Espacio_H"
    df_u = df_u.copy()
    df_u["_fecha_efectiva"] = df_u["Fecha de Inventario"].combine_first(df_u["Espacio_H"])

    df_actual = (
        df_u.sort_values("_fecha_efectiva", ascending=True, na_position="first")
        .drop_duplicates(subset=["Unidad de Negocio", "Nombre del Insumo"], keep="last")
        .copy()
    )

    for col in ["Alm", "Barra", "Stock Neto", "Stock Mínimo"]:
        df_actual[col] = df_actual[col].apply(limpiar_valor)

    df_actual["Stock Neto Calculado"] = df_actual["Alm"] + df_actual["Barra"]

    if "Necesita Compra" in df_actual.columns:
        df_actual["Necesita Compra"] = (
            df_actual["Necesita Compra"].astype(str).str.strip().str.upper() == "TRUE"
        )
    else:
        df_actual["Necesita Compra"] = df_actual["Stock Neto Calculado"] < df_actual["Stock Mínimo"]

    # Copiar "_fecha_efectiva" a Fecha de Inventario para display uniforme
    df_actual["Fecha de Inventario"] = df_actual["_fecha_efectiva"]
    df_actual.drop(columns=["_fecha_efectiva"], inplace=True, errors="ignore")

    return df_actual


def fecha_max_segura(serie: pd.Series) -> str:
    """Retorna el máximo de una serie de timestamps como string, sin crashear en NaT."""
    validas = serie.dropna()
    if validas.empty:
        return "Sin registros"
    return validas.max().strftime("%d/%m %H:%M")


# ============================================================
# ESTADO DE SESIÓN — inicialización única y segura
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

# responsables se inicializa SIEMPRE desde la fuente de verdad, no sobreescribiendo sesión activa
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
**Rutina Diaria (Perecederos):**
* **Grupo A:** Café, Leches y Lácteos
* **Grupo B:** Jarabes y Salsas
* **Grupo C:** Polvos, Tés y Tisanas

**Rutina 2 Días (Secos y Suministros):**
* **Grupo D:** Empaques y Desechables
* **Grupo E:** Suministros de Limpieza
* **Grupo F:** Comida y Vitrina
* **Grupo G:** Otros (Retail / Utensilios)
        """)

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
                            nuevo_df = DF_USUARIOS.copy()
                            nuevo_df = nuevo_df[nuevo_df["Nombre"] != n_nombre]
                            nueva_fila = pd.DataFrame([{"Clave": str(n_clave), "Nombre": n_nombre, "Rol": n_rol}])
                            nuevo_df = pd.concat([nuevo_df, nueva_fila], ignore_index=True)
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

    # --- CATÁLOGO (solo autenticados) ---
    if st.session_state.auth_status:
        st.divider()
        st.subheader("🛠️ Gestión del Catálogo")
        op_cat = st.radio("Acción:", ["Añadir Insumo", "Editar Insumo"])

        if op_cat == "Añadir Insumo":
            with st.form("f_add", clear_on_submit=True):
                u  = st.selectbox("Unidad", UNIDADES)
                n  = st.text_input("Nombre del Insumo")
                m  = st.text_input("Marca")
                p  = st.text_input("Proveedor")            # CORRECCIÓN: st. agregado
                g  = st.selectbox("Grupo", GRUPOS)
                uc = st.text_input("Presentación de Compra")
                um = st.selectbox("Unidad de Medida", UNIDADES_MED)
                sm = st.number_input("Stock Mínimo", min_value=0.0)

                if st.form_submit_button("✨ Crear Insumo"):
                    if not n.strip():
                        st.error("El nombre del insumo es obligatorio.")
                    else:
                        ws_ins, err = safe_worksheet(sh, "Insumos")
                        if err:
                            st.error(err)
                        else:
                            try:
                                nueva_fila = [u, n.strip(), m, p, g, "", uc, um, "", "", "", sm]
                                ws_ins.append_row(nueva_fila, value_input_option="USER_ENTERED")
                                st.cache_data.clear()
                                st.success(f"Insumo '{n}' creado.")
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error al crear insumo: {e}")

        else:  # Editar Insumo — CORRECCIÓN: bloque separado (no anidado)
            if df_raw.empty or "Nombre del Insumo" not in df_raw.columns:
                st.info("Sin insumos disponibles para editar.")
            else:
                ins_nombres = df_raw["Nombre del Insumo"].dropna().unique().tolist()
                if not ins_nombres:
                    st.info("El catálogo está vacío.")
                else:
                    ins_edit = st.selectbox("Seleccionar Insumo a Editar:", sorted(ins_nombres))
                    mask = df_raw["Nombre del Insumo"] == ins_edit
                    if not mask.any():
                        st.warning("Insumo no encontrado en el catálogo actual.")
                    else:
                        d = df_raw[mask].iloc[0]  # Seguro: ya verificamos mask.any()

                        with st.form("f_edit"):
                            e_u  = st.selectbox("Unidad", UNIDADES,
                                                index=UNIDADES.index(d.get("Unidad de Negocio", UNIDADES[0]))
                                                if d.get("Unidad de Negocio") in UNIDADES else 0)
                            e_n  = st.text_input("Nombre",  value=str(d.get("Nombre del Insumo", "")))
                            e_m  = st.text_input("Marca",   value=str(d.get("Marca", "")))
                            e_p  = st.text_input("Proveedor", value=str(d.get("Proveedor", "")))

                            grupo_val = str(d.get("Grupo", "A"))
                            e_g = st.selectbox("Grupo", GRUPOS,
                                               index=GRUPOS.index(grupo_val) if grupo_val in GRUPOS else 0)

                            e_uc = st.text_input("Presentación Compra",
                                                 value=str(d.get("Presentación de Compra", "")))

                            u_val = str(d.get("Unidad de Medida", "pz")).lower()
                            e_um = st.selectbox("Medida", UNIDADES_MED,
                                                index=UNIDADES_MED.index(u_val) if u_val in UNIDADES_MED else 0)

                            e_sm = st.number_input("Stock Mínimo",
                                                   min_value=0.0,
                                                   value=limpiar_valor(d.get("Stock Mínimo", 0)))

                            if st.form_submit_button("💾 Actualizar Insumo"):
                                if not e_n.strip():
                                    st.error("El nombre no puede quedar vacío.")
                                else:
                                    ws_ins, err = safe_worksheet(sh, "Insumos")
                                    if err:
                                        st.error(err)
                                    else:
                                        try:
                                            # Sheet_Row_Num fue asignado en cargar_datos_integrales
                                            idx = int(d.get("Sheet_Row_Num", 0))
                                            if idx < 2:
                                                raise ValueError("Número de fila inválido.")
                                            fila_act = [[e_u, e_n.strip(), e_m, e_p, e_g, "", e_uc, e_um, "", "", "", e_sm]]
                                            ws_ins.update(range_name=f"A{idx}:L{idx}", values=fila_act)
                                            st.cache_data.clear()
                                            st.success("Catálogo actualizado.")
                                            time.sleep(1)
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Error al actualizar: {e}")


# ============================================================
# PÁGINAS PRINCIPALES
# ============================================================
pagina = st.session_state.pagina

# ------ DASHBOARD ------
if pagina == "Dashboard":
    st.title("📊 Dashboard Operativo")

    ahora = datetime.now()
    dias_faltantes = calendar.monthrange(ahora.year, ahora.month)[1] - ahora.day
    if dias_faltantes <= 4:
        st.info(f"⏳ A {dias_faltantes} días del fin de mes. Recuerda ejecutar el **Corte de Mes**.")

    df_actual = obtener_ultimo_inventario(df_historial)

    if not df_actual.empty:
        crit = df_actual[df_actual["Necesita Compra"] == True]

        c1, c2, c3 = st.columns(3)
        c1.metric("🛒 Pendientes Noble",          len(crit[crit["Unidad de Negocio"] == "Noble"]))
        c2.metric("🛒 Pendientes Coffee Station",  len(crit[crit["Unidad de Negocio"] == "Coffee Station"]))
        # CORRECCIÓN: fecha_max_segura evita crash en .strftime() con NaT
        c3.metric("🕒 Último Movimiento", fecha_max_segura(df_actual["Fecha de Inventario"]))

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🏢 Faltantes: Noble")
            ins_n = crit[crit["Unidad de Negocio"] == "Noble"]
            if not ins_n.empty:
                for _, r in ins_n.iterrows():
                    st.error(f"**{r['Nombre del Insumo']}** (Stock: {r['Stock Neto Calculado']} / Mín: {r['Stock Mínimo']})")
            else:
                st.success("Operación cubierta.")
        with col2:
            st.subheader("☕ Faltantes: Coffee Station")
            ins_cs = crit[crit["Unidad de Negocio"] == "Coffee Station"]
            if not ins_cs.empty:
                for _, r in ins_cs.iterrows():
                    st.error(f"**{r['Nombre del Insumo']}** (Stock: {r['Stock Neto Calculado']} / Mín: {r['Stock Mínimo']})")
            else:
                st.success("Operación cubierta.")

        st.divider()
        st.subheader("🕒 Actividad Reciente")
        df_log = df_historial.copy()
        df_log["Fecha de Inventario"] = df_log["Fecha de Inventario"].combine_first(df_log["Espacio_H"])
        cols_log = ["Fecha de Inventario", "Responsable", "Unidad de Negocio",
                    "Nombre del Insumo", "Stock Neto", "Necesita Compra", "Comentarios"]
        cols_log_existentes = [c for c in cols_log if c in df_log.columns]
        st.dataframe(
            df_log.dropna(subset=["Fecha de Inventario"])
                  .sort_values("Fecha de Inventario", ascending=False)[cols_log_existentes]
                  .head(15),
            use_container_width=True
        )
    else:
        st.info("Sin datos históricos. Ejecuta el primer conteo de inventario.")


# ------ CAPTURAR INVENTARIO ------
elif pagina == "Inventario":
    st.title("📝 Capturar inventario")

    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida.")
        st.stop()

    col_u, col_r, col_g = st.columns([1, 1, 2])
    with col_u:
        u_sel = st.selectbox("🏢 Unidad de Negocio", UNIDADES)

    responsables = st.session_state.responsables or ["Raúl"]
    resp_idx = responsables.index(st.session_state.current_user) \
        if st.session_state.current_user in responsables else 0

    with col_r:
        r_sel = st.selectbox(
            "👤 Responsable", responsables, index=resp_idx,
            disabled=(st.session_state.user_role != "admin")
        )

    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()

    with col_g:
        grps = sorted(df_u["Grupo"].dropna().unique().tolist()) \
            if not df_u.empty and "Grupo" in df_u.columns else GRUPOS
        g_sel = st.multiselect("📂 Grupos a contar", grps, default=grps[:1] if grps else [])

    st.divider()
    busqueda_inv = st.text_input("🔍 Buscar insumo:", placeholder="Escribe el nombre...")

    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    df_f = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"]).reset_index(drop=True) \
        if not df_u.empty and g_sel else pd.DataFrame()

    if busqueda_inv and not df_f.empty:
        df_f = df_f[df_f["Nombre del Insumo"].astype(str).str.contains(busqueda_inv, case=False, na=False)]

    if df_f.empty:
        st.info("Selecciona al menos un grupo para mostrar insumos.")
    else:
        regs = {}
        h1, h2, h3, h4, h5, h6, h7 = st.columns([2.2, 0.8, 0.8, 0.8, 0.8, 1.0, 2.0])
        for col, label in zip([h1,h2,h3,h4,h5,h6,h7],
                              ["Insumo / Ref","Almacén","Barra","Medida","Neto","¿Pedir?","Comentarios"]):
            col.write(f"**{label}**")
        st.divider()

        for _, row in df_f.iterrows():
            nom      = str(row.get("Nombre del Insumo", ""))
            safe_nom = nom.replace(" ", "_").replace('"', "").replace("'", "")
            v_prev   = 0.0
            if not df_actual.empty and "Nombre del Insumo" in df_actual.columns:
                match = df_actual[df_actual["Nombre del Insumo"] == nom]
                if not match.empty:
                    v_prev = match.iloc[0]["Stock Neto Calculado"]
            v_min = limpiar_valor(row.get("Stock Mínimo", 0))

            c1, c2, c3, c4, c5, c6, c7 = st.columns([2.2, 0.8, 0.8, 0.8, 0.8, 1.0, 2.0])
            with c1:
                st.write(f"**{nom}**")
                st.caption(f"Marca: {row.get('Marca','-')} | Prov: {row.get('Proveedor','-')}")
                diff  = v_prev - v_min
                color = "green" if diff >= 0 else "red"
                st.markdown(
                    f"<small>Anterior: {v_prev} | Mín: {v_min} "
                    f"(<span style='color:{color}'>{diff:+.1f}</span>)</small>",
                    unsafe_allow_html=True
                )
            with c2: v_a = st.number_input("Alm", min_value=0.0, step=1.0, key=f"a_{safe_nom}", label_visibility="collapsed")
            with c3: v_b = st.number_input("Bar", min_value=0.0, step=1.0, key=f"b_{safe_nom}", label_visibility="collapsed")
            with c4:
                u_act = str(row.get("Unidad de Medida", "pz")).lower()
                v_u = st.selectbox("U", UNIDADES_MED,
                                   index=UNIDADES_MED.index(u_act) if u_act in UNIDADES_MED else 0,
                                   key=f"u_{safe_nom}", label_visibility="collapsed")
            with c5:
                v_n = v_a + v_b
                st.write(f"**{v_n:.1f}**")
            with c6:
                # Lógica del toggle: auto-calcula, permite override manual
                p_key = f"p_{safe_nom}"
                manual_key  = f"m_{safe_nom}"
                last_math_k = f"lm_{safe_nom}"

                if manual_key  not in st.session_state: st.session_state[manual_key]  = None
                if last_math_k not in st.session_state: st.session_state[last_math_k] = None

                math_val = bool(v_n < v_min)
                if st.session_state[last_math_k] != math_val:
                    st.session_state[manual_key]  = None
                    st.session_state[last_math_k] = math_val

                def make_cb(k, mk):
                    def cb(): st.session_state[mk] = st.session_state[k]
                    return cb

                if st.session_state[manual_key] is None:
                    st.session_state[p_key] = math_val

                v_p = st.toggle("🛒", key=p_key, on_change=make_cb(p_key, manual_key))
            with c7:
                v_c = st.text_input("Nota...", key=f"c_{safe_nom}",
                                    label_visibility="collapsed", placeholder="Opcional")

            regs[nom] = {"a": v_a, "b": v_b, "n": v_n, "u": v_u, "p": v_p, "c": v_c, "row": row}
            st.divider()

        if st.button("📥 PROCESAR INVENTARIO", use_container_width=True, type="primary"):
            if not regs:
                st.warning("No hay insumos para procesar.")
            else:
                ws_his, err = safe_worksheet(sh, "Historial")
                if err:
                    st.error(err)
                else:
                    fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    filas = []
                    for n, info in regs.items():
                        dm = info["row"]
                        filas.append([
                            u_sel, n, dm.get("Marca",""), dm.get("Proveedor",""),
                            dm.get("Grupo",""), "", dm.get("Presentación de Compra",""),
                            info["u"], info["a"], info["b"], info["n"],
                            dm.get("Stock Mínimo", 0), "TRUE" if info["p"] else "FALSE",
                            r_sel, fh, info["c"]
                        ])
                    ok, msg = append_rows_con_retry(ws_his, filas)
                    if ok:
                        st.cache_data.clear()
                        st.success(f"¡Inventario procesado! {msg}")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)


# ------ ENTRADA DE COMPRAS ------
elif pagina == "Ingresos":
    st.title("📥 Entrada de compras")

    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida.")
        st.stop()

    st.info("Ingresa insumos recibidos. Se sumarán al último stock de Almacén registrado.")

    col_u, col_r = st.columns(2)
    with col_u:
        u_sel = st.selectbox("🏢 Unidad receptora:", UNIDADES)

    responsables = st.session_state.responsables or ["Raúl"]
    resp_idx = responsables.index(st.session_state.current_user) \
        if st.session_state.current_user in responsables else 0
    with col_r:
        r_sel = st.selectbox("👤 Responsable:", responsables, index=resp_idx,
                             disabled=(st.session_state.user_role != "admin"))

    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()

    if df_u.empty:
        st.warning("Sin insumos registrados para esta unidad.")
        st.stop()

    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    nombres_insumos = df_u["Nombre del Insumo"].dropna().unique().tolist()

    st.divider()
    modo_bulk = st.toggle("🚀 Activar Ingreso Masivo Rápido (Bulk)")

    if modo_bulk:
        st.subheader("Carga Bulk")
        bulk_data = []
        for _, r in df_u.iterrows():
            nom = r["Nombre del Insumo"]
            v_a_prev, v_b_prev = 0.0, 0.0
            if not df_actual.empty:
                m = df_actual[df_actual["Nombre del Insumo"] == nom]
                if not m.empty:
                    v_a_prev = m.iloc[0]["Alm"]
                    v_b_prev = m.iloc[0]["Barra"]
            bulk_data.append({
                "Insumo": nom, "Stock Alm": v_a_prev,
                "Stock Barra": v_b_prev, "+ Ingreso": 0.0
            })

        df_edit = pd.DataFrame(bulk_data)
        edited_df = st.data_editor(
            df_edit[["Insumo", "Stock Alm", "Stock Barra", "+ Ingreso"]],
            hide_index=True, use_container_width=True,
            disabled=["Insumo", "Stock Alm", "Stock Barra"]
        )

        if st.button("📦 EJECUTAR INGRESO BULK", type="primary"):
            ws_his, err = safe_worksheet(sh, "Historial")
            if err:
                st.error(err)
            else:
                fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                filas_bulk = []
                for _, r_ed in edited_df.iterrows():
                    ingreso = limpiar_valor(r_ed["+ Ingreso"])
                    if ingreso <= 0:
                        continue
                    nom = r_ed["Insumo"]
                    # CORRECCIÓN: next() con default=None + validación
                    orig = next((item for item in bulk_data if item["Insumo"] == nom), None)
                    if orig is None:
                        continue
                    row_insumo_matches = df_u[df_u["Nombre del Insumo"] == nom]
                    if row_insumo_matches.empty:
                        continue
                    row_insumo = row_insumo_matches.iloc[0]
                    v_min   = limpiar_valor(row_insumo.get("Stock Mínimo", 0))
                    nuevo_a = orig["Stock Alm"] + ingreso
                    nuevo_n = nuevo_a + orig["Stock Barra"]
                    filas_bulk.append([
                        u_sel, nom, row_insumo.get("Marca",""), row_insumo.get("Proveedor",""),
                        row_insumo.get("Grupo",""), fh,
                        row_insumo.get("Presentación de Compra",""),
                        row_insumo.get("Unidad de Medida","pz"),
                        nuevo_a, orig["Stock Barra"], nuevo_n, v_min,
                        "TRUE" if nuevo_n < v_min else "FALSE", r_sel, "", ""
                    ])

                if not filas_bulk:
                    st.warning("No ingresaste cantidades mayores a 0.")
                else:
                    ok, msg = append_rows_con_retry(ws_his, filas_bulk)
                    if ok:
                        st.cache_data.clear()
                        st.success(f"Ingreso masivo registrado: {len(filas_bulk)} referencias. {msg}")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)

    else:
        insumos_llegados = st.multiselect("🔍 Insumos recibidos:", sorted(nombres_insumos))
        if insumos_llegados:
            regs_ingreso = {}
            st.divider()
            h1, h2, h3, h4 = st.columns([3, 2, 2, 2])
            for col, label in zip([h1,h2,h3,h4], ["Insumo","Stock Ant (Alm+Bar)","+ Cantidad","= Nuevo Total"]):
                col.write(f"**{label}**")
            st.divider()

            for i, nom in enumerate(insumos_llegados):
                row_matches = df_u[df_u["Nombre del Insumo"] == nom]
                if row_matches.empty:
                    continue
                row_insumo = row_matches.iloc[0]
                v_a_prev, v_b_prev = 0.0, 0.0
                if not df_actual.empty:
                    m = df_actual[df_actual["Nombre del Insumo"] == nom]
                    if not m.empty:
                        v_a_prev = m.iloc[0]["Alm"]
                        v_b_prev = m.iloc[0]["Barra"]
                v_min = limpiar_valor(row_insumo.get("Stock Mínimo", 0))

                c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                with c1:
                    st.write(f"**{nom}**")
                    st.caption(f"Marca: {row_insumo.get('Marca','-')} | Prov: {row_insumo.get('Proveedor','-')}")
                with c2:
                    st.write(f"Almacén: {v_a_prev} | Barra: {v_b_prev}")
                    st.write(f"**Total Ant: {v_a_prev + v_b_prev}**")
                with c3:
                    cant_ingreso = st.number_input("Ingreso", min_value=0.0, step=1.0,
                                                   key=f"ing_{i}", label_visibility="collapsed")
                with c4:
                    nuevo_alm  = v_a_prev + cant_ingreso
                    nuevo_neto = nuevo_alm + v_b_prev
                    st.success(f"**{nuevo_neto:.1f}**")

                regs_ingreso[nom] = {
                    "nuevo_a": nuevo_alm, "b": v_b_prev,
                    "nuevo_n": nuevo_neto, "row": row_insumo, "min": v_min
                }
                st.divider()

            if st.button("📦 EJECUTAR INGRESO", use_container_width=True, type="primary"):
                ws_his, err = safe_worksheet(sh, "Historial")
                if err:
                    st.error(err)
                else:
                    fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    filas = []
                    for n, info in regs_ingreso.items():
                        dm = info["row"]
                        filas.append([
                            u_sel, n, dm.get("Marca",""), dm.get("Proveedor",""),
                            dm.get("Grupo",""), fh,
                            dm.get("Presentación de Compra",""), dm.get("Unidad de Medida","pz"),
                            info["nuevo_a"], info["b"], info["nuevo_n"], info["min"],
                            "TRUE" if info["nuevo_n"] < info["min"] else "FALSE", r_sel, "", ""
                        ])
                    ok, msg = append_rows_con_retry(ws_his, filas)
                    if ok:
                        st.cache_data.clear()
                        st.success(f"Ingreso registrado. {msg}")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)


# ------ INVENTARIO ACTUAL ------
elif pagina == "Consulta":
    st.title("📦 Inventario actual")
    u_sel    = st.selectbox("🏢 Unidad:", UNIDADES)
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
        col_prov = "Proveedor_H" if "Proveedor_H" in df_actual.columns else None
        if col_prov:
            provs = ["Todos"] + sorted(df_actual[col_prov].dropna().unique().tolist())
            prov_sel = st.selectbox("🚛 Filtro Proveedor:", provs)
        else:
            prov_sel = "Todos"

    df_display = df_actual.copy()
    if busqueda:
        df_display = df_display[df_display["Nombre del Insumo"].astype(str).str.contains(busqueda, case=False, na=False)]
    if prov_sel != "Todos" and col_prov:
        df_display = df_display[df_display[col_prov] == prov_sel]

    col_map = {
        "Grupo_H": "Grupo", "Nombre del Insumo": "Insumo", "Marca_H": "Marca",
        "Proveedor_H": "Proveedor", "Alm": "Almacén", "Barra": "Barra",
        "Stock Neto Calculado": "Stock Total", "Unidad_Medida_H": "Medida",
        "Stock Mínimo": "Mínimo", "Fecha de Inventario": "Último Corte", "Comentarios": "Comentarios"
    }
    cols_existentes = [c for c in col_map if c in df_display.columns]
    df_final = df_display[cols_existentes].rename(columns=col_map)

    def highlight_low(row):
        total = row.get("Stock Total", 9999)
        minimo = row.get("Mínimo", 0)
        color = "background-color: rgba(255, 75, 75, 0.2)" if total < minimo else ""
        return [color] * len(row)

    st.dataframe(df_final.style.apply(highlight_low, axis=1), use_container_width=True, hide_index=True)

    st.divider()
    csv = df_final.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Descargar Reporte (CSV)", data=csv,
        file_name=f"Inventario_{u_sel}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv", use_container_width=True
    )


# ------ TICKETS ------
elif pagina == "Impresion":
    st.title("🖨️ Ticket de Conteo (58mm)")
    u_sel = st.selectbox("Sucursal:", UNIDADES)
    df_u  = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    grps  = sorted(df_u["Grupo"].dropna().unique().tolist()) \
        if not df_u.empty and "Grupo" in df_u.columns else []
    g_sel = st.multiselect("Filtrar por Grupos:", grps)

    if g_sel and not df_u.empty:
        df_p = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"])
        t  = f"*** CONTEO {u_sel.upper()} ***\n"
        t += f"Fecha: {datetime.now().strftime('%d/%m/%Y')}\n"
        t += "-" * 22 + "\n"
        for _, r in df_p.iterrows():
            t += f" {str(r['Nombre del Insumo'])[:20]}\n"
            t += f" [   ] Alm [   ] Bar\n"
            t += "-" * 22 + "\n"
        st.info("Copia el texto para imprimir o enviar a la impresora térmica.")
        st.code(t, language=None)
    else:
        st.info("Selecciona grupos para generar la lista.")


elif pagina == "ListaCompra":
    st.title("🛒 Ticket de Compra (58mm)")
    u_sel    = st.radio("Generar orden para:", UNIDADES, horizontal=True)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)

    if df_actual.empty:
        st.info("Sin registros para armar la lista de compra.")
        st.stop()

    com = df_actual[df_actual["Necesita Compra"] == True]
    if not com.empty:
        t  = f"*** COMPRAS {u_sel.upper()} ***\n"
        t += f"Fecha: {datetime.now().strftime('%d/%m/%Y')}\n"
        t += "-" * 22 + "\n"
        for _, r in com.iterrows():
            t += f"• {str(r['Nombre del Insumo'])[:20]}\n"
            t += f"  Stock: {r['Stock Neto Calculado']} / Min: {r['Stock Mínimo']}\n"
            t += "-" * 22 + "\n"
        st.info("Copia el texto para imprimir.")
        st.code(t, language=None)
    else:
        st.success("No hay alertas de reabastecimiento activas.")


elif pagina == "ReporteStock":
    st.title("📦 Reporte de Stock (58mm)")
    u_sel    = st.radio("Generar reporte para:", UNIDADES, horizontal=True)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)

    if df_actual.empty:
        st.warning("Sin registros para generar el reporte.")
        st.stop()

    t  = f"*** INVENTARIO {u_sel.upper()} ***\n"
    t += f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
    t += "-" * 22 + "\n"
    col_grupo = "Grupo_H" if "Grupo_H" in df_actual.columns else "Grupo"
    df_rep    = df_actual.sort_values([col_grupo, "Nombre del Insumo"])
    grupo_actual = ""
    for _, r in df_rep.iterrows():
        grupo = str(r.get(col_grupo, ""))
        if grupo != grupo_actual:
            t += f"\n>> GRUPO {grupo} <<\n"
            grupo_actual = grupo
        t += f"{str(r['Nombre del Insumo'])[:20]}\n"
        t += f" Alm:{r['Alm']} Bar:{r['Barra']} Total:{r['Stock Neto Calculado']}\n"
    t += "-" * 22 + "\n"
    st.info("Copia el texto para imprimir.")
    st.code(t, language=None)


# ------ CORTE DE MES ------
elif pagina == "CorteMes":
    if st.session_state.user_role != "admin":
        st.error("🚫 Acceso denegado. Solo administradores.")
        st.stop()

    st.title("🔒 Corte de Mes")
    st.warning(
        "Este proceso consolidará el stock actual como saldo inicial y archivará "
        "los registros previos. **Acción irreversible.** Asegúrate de que todos los "
        "conteos del día estén registrados antes de continuar."
    )

    confirmar = st.checkbox("Confirmo que deseo ejecutar el cierre de mes.")

    if confirmar and st.button("🚀 Ejecutar Cierre", type="primary"):
        with st.status("Ejecutando protocolo de cierre...", expanded=True) as status:
            try:
                # PASO 1: Calcular estados finales
                st.write("1/4 — Calculando estados finales de stock...")
                df_corte = obtener_ultimo_inventario(df_historial)
                if df_corte.empty:
                    st.error("No hay datos de inventario para cerrar.")
                    st.stop()

                fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                encabezados = COLS_HISTORIAL
                col_grupo = "Grupo_H" if "Grupo_H" in df_corte.columns else "Grupo"
                col_prov  = "Proveedor_H" if "Proveedor_H" in df_corte.columns else "Proveedor"

                filas_corte = []
                for _, r in df_corte.iterrows():
                    filas_corte.append([
                        r.get("Unidad de Negocio",""), r.get("Nombre del Insumo",""),
                        r.get("Marca_H",""),            r.get(col_prov,""),
                        r.get(col_grupo,""),            "",
                        r.get("Pres_Compra_H",""),      r.get("Unidad_Medida_H",""),
                        r.get("Alm", 0),                r.get("Barra", 0),
                        r.get("Stock Neto Calculado",0), r.get("Stock Mínimo", 0),
                        "TRUE" if r.get("Necesita Compra", False) else "FALSE",
                        "SISTEMA-CIERRE", fh, "Corte consolidado"
                    ])

                # PASO 2: Archivar historial ANTES de limpiar
                st.write("2/4 — Archivando historial previo...")
                ws_his, err = safe_worksheet(sh, "Historial")
                if err:
                    raise RuntimeError(err)

                datos_hist = ws_his.get_all_values()

                try:
                    ws_arc, _ = safe_worksheet(sh, "Archivo_Historial")
                    if ws_arc is None:
                        ws_arc = sh.add_worksheet(title="Archivo_Historial", rows="5000", cols="20")
                        ws_arc.append_row(encabezados)
                except Exception:
                    ws_arc = sh.add_worksheet(title="Archivo_Historial", rows="5000", cols="20")
                    ws_arc.append_row(encabezados)

                if len(datos_hist) > 1:
                    ws_arc.append_rows(datos_hist[1:])

                # PASO 3: Escribir Cierres con el consolidado
                st.write("3/4 — Consolidando saldos iniciales...")
                try:
                    ws_cie, _ = safe_worksheet(sh, "Cierres")
                    if ws_cie is None:
                        ws_cie = sh.add_worksheet(title="Cierres", rows="1000", cols="20")
                    ws_cie.clear()
                except Exception:
                    ws_cie = sh.add_worksheet(title="Cierres", rows="1000", cols="20")

                ws_cie.append_row(encabezados)
                ws_cie.append_rows(filas_corte)

                # PASO 4: Limpiar Historial (ÚLTIMO paso — si falla, Cierres ya tiene los datos)
                st.write("4/4 — Reiniciando Historial...")
                ws_his.clear()
                ws_his.append_row(encabezados)

                st.cache_data.clear()
                status.update(label="✅ Cierre completado", state="complete")
                st.success(
                    "Base de datos optimizada. Historial anterior en 'Archivo_Historial'. "
                    f"Saldos consolidados en 'Cierres' ({len(filas_corte)} referencias)."
                )
                time.sleep(2)
                st.rerun()

            except Exception as e:
                status.update(label="❌ Falla en el cierre", state="error")
                st.error(
                    f"Error durante el cierre: {e}\n\n"
                    "El historial NO fue eliminado. Revisa la pestaña 'Cierres' en el Spreadsheet "
                    "antes de reintentar."
                )
