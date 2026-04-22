import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime
import time
import calendar
import unicodedata
import re

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
]

COLS_HISTORIAL = [
    "Unidad de Negocio",      # A
    "Nombre del Insumo",      # B
    "Marca_H",                # C
    "Proveedor_H",            # D
    "Grupo_H",                # E
    "Espacio_H",              # F
    "Pres_Compra_H",          # G
    "Unidad_Medida_H",        # H
    "Alm",                    # I
    "Barra",                  # J
    "Stock Neto",             # K
    "Stock Mínimo",           # L
    "Necesita Compra",        # M
    "Responsable",            # N
    "Fecha de Inventario",    # O
    "Comentarios",            # P
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
# HELPERS UTILITARIOS
# ============================================================

def limpiar_valor(valor) -> float:
    """
    Convierte cualquier valor de celda a float de forma segura.
    Retorna 0.0 solo si el valor realmente es vacío/nulo/inválido.
    (Versión mejorada fusionando la robustez de Gemini)
    """
    if pd.isna(valor) or valor is None or str(valor).strip() == "":
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    try:
        s = str(valor).replace('%', '').replace('$', '').replace(',', '').strip()
        return float(s)
    except (ValueError, TypeError):
        return 0.0

def normalizar_nombre(nombre) -> str:
    """
    Normaliza un nombre de insumo para comparaciones robustas.
    Elimina acentos, espacios extra y convierte a minúsculas.
    """
    s = str(nombre).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'\s+', ' ', s)
    return s

def normalizar_dataframe(df: pd.DataFrame, columnas_esperadas: list,
                         cols_criticas: set = None) -> pd.DataFrame:
    """
    Mapea columnas por NOMBRE (no por posición).
    """
    if df.empty:
        return pd.DataFrame(columns=columnas_esperadas)

    df = df.copy()
    cols_en_sheet    = set(df.columns)
    cols_faltantes   = [c for c in columnas_esperadas if c not in cols_en_sheet]
    
    if cols_criticas:
        faltantes_criticas = set(cols_faltantes) & cols_criticas
        if faltantes_criticas:
            st.warning(
                f"⚠️ Columnas críticas no encontradas en el Sheet: {sorted(faltantes_criticas)}. "
                "Verifica que los encabezados del Sheet coincidan exactamente con la configuración."
            )

    for col in cols_faltantes:
        df[col] = None

    return df[columnas_esperadas]

def safe_worksheet(sh, nombre: str):
    """
    Obtiene un worksheet con validación explícita.
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
# CAPA DE CONEXIÓN
# ============================================================
st.set_page_config(page_title="Noble & Coffee Station", page_icon="☕", layout="wide")

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
# ============================================================
@st.cache_data(ttl=30)
def cargar_datos_integrales():
    """
    Carga Insumos + Historial + Cierres desde Google Sheets.
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
        val_cie = ws_cie.get_all_values() if ws_cie else []

        def _to_df(vals):
            if len(vals) > 1:
                return pd.DataFrame(vals[1:], columns=vals[0])
            return pd.DataFrame(columns=vals[0] if vals else [])

        df_ins = _to_df(val_ins)
        df_his = _to_df(val_his)
        df_cie = _to_df(val_cie) if val_cie else pd.DataFrame()

        df_ins["Sheet_Row_Num"] = df_ins.index + 2
        df_ins = normalizar_dataframe(df_ins, COLS_INSUMOS + ["Sheet_Row_Num"], cols_criticas=COLS_CRITICAS_INSUMOS)
        df_his = normalizar_dataframe(df_his, COLS_HISTORIAL, cols_criticas=COLS_CRITICAS_HISTORIAL)

        if not df_cie.empty:
            df_cie = normalizar_dataframe(df_cie, COLS_HISTORIAL)
            df_total = pd.concat([df_cie, df_his], ignore_index=True)
        else:
            df_total = df_his

        if not df_total.empty:
            df_total["Fecha de Inventario"] = pd.to_datetime(df_total["Fecha de Inventario"], errors="coerce")
            df_total["Espacio_H"] = pd.to_datetime(df_total["Espacio_H"], errors="coerce")

        return df_ins, df_total

    except Exception as e:
        st.error(f"Falla en extracción de datos: {e}")
        return pd.DataFrame(), pd.DataFrame()
        @st.cache_data(ttl=60)
def obtener_usuarios():
    """Carga Base de Accesos"""
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
            for _, r in df_usr.iterrows() if str(r.get("Clave", "")).strip()
        }
        lista_nombres = df_usr["Nombre"].dropna().tolist()
        
        return usuarios_dict, lista_nombres, df_usr
    except Exception as e:
        st.warning(f"Error cargando usuarios: {e}")
        return {}, [], pd.DataFrame()

USUARIOS_PIN, LISTA_RESPONSABLES, DF_USUARIOS = obtener_usuarios()

# ============================================================
# LÓGICA DE NEGOCIO Y ESTADO DE SESIÓN
# ============================================================
def obtener_ultimo_inventario(df_hist: pd.DataFrame, unidad: str = None) -> pd.DataFrame:
    if df_hist.empty:
        return pd.DataFrame()
        
    df_u = df_hist.copy()
    if unidad:
        df_u = df_u[df_u["Unidad de Negocio"] == unidad]
        
    if df_u.empty:
        return pd.DataFrame()

    df_u["_fecha_efectiva"] = df_u["Fecha de Inventario"].combine_first(df_u["Espacio_H"])
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

    if "Necesita Compra" in df_actual.columns:
        df_actual["Necesita Compra"] = df_actual["Necesita Compra"].astype(str).str.strip().str.upper() == "TRUE"
    else:
        df_actual["Necesita Compra"] = df_actual["Stock Neto Calculado"] < df_actual["Stock Mínimo"]

    df_actual["Fecha de Inventario"] = df_actual["_fecha_efectiva"]
    df_actual.drop(columns=["_fecha_efectiva", "_nombre_norm"], inplace=True, errors="ignore")
    
    return df_actual

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
                    st.session_state.auth_status = True
                    st.session_state.current_user = USUARIOS_PIN[pin_input]["nombre"]
                    st.session_state.user_role = USUARIOS_PIN[pin_input]["rol"]
                    st.rerun()
                else:
                    st.error("⚠️ Clave incorrecta o no registrada.")
    else:
        st.write(f"👤 Operador: **{st.session_state.current_user}**")
        
        if st.button("🚪 Cerrar Sesión", use_container_width=True):
            for k in ["auth_status", "current_user", "user_role"]:
                st.session_state[k] = None
            st.session_state.auth_status = False
            cambiar_pagina("Dashboard")
        
    st.divider()
    st.title("⚙️ Operaciones Noble")
    
    if st.button("📊 Dashboard Principal", use_container_width=True):
        cambiar_pagina("Dashboard")
    
    st.divider()
    st.write("**📦 Movimientos de Stock:**")
    if st.button("📝 1. Capturar inventario", use_container_width=True):
        cambiar_pagina("Inventario")
    if st.button("📥 2. Entrada de compras", use_container_width=True):
        cambiar_pagina("Ingresos")
    if st.button("📦 3. Inventario actual", use_container_width=True):
        cambiar_pagina("Consulta")
    
    st.divider()
    st.write("**🖨️ Tickets (58mm):**")
    if st.button("📋 1. Lista de Conteo", use_container_width=True):
        cambiar_pagina("Impresion")
    if st.button("🛒 2. Lista de Compra", use_container_width=True):
        cambiar_pagina("ListaCompra")
    if st.button("📦 3. Reporte de Stock", use_container_width=True):
        cambiar_pagina("ReporteStock")
    
    # --- TABLA DE GRUPOS ACTUALIZADA CON EL 100% DE INFORMACIÓN ---
    st.divider()
    with st.expander("ℹ️ Guía de Clasificación (Grupos)"):
        st.markdown("**SISTEMA LOGÍSTICO NOBLE**")
        data_grupos = [
            {"Grupo": "A", "Categoría": "Café y Lácteos", "Frecuencia": "Diaria", "Detalles": "Granos de café, Leches (Entera, Deslactosada, Vegetales), Cremas."},
            {"Grupo": "B", "Categoría": "Jarabes y Salsas", "Frecuencia": "Diaria", "Detalles": "Saborizantes, Salsas (Chocolate/Caramelo), Purés, Concentrados."},
            {"Grupo": "C", "Categoría": "Polvos y Tés", "Frecuencia": "Diaria", "Detalles": "Matcha, Taro, Chai, Bases Frappé, Cacao, Azúcar, Tisanas."},
            {"Grupo": "D", "Categoría": "Empaques / Desechables", "Frecuencia": "2 Días", "Detalles": "Vasos, Tapas, Mangas, Servilletas, Popotes, Agitadores, Bolsas Kraft."},
            {"Grupo": "E", "Categoría": "Limpieza", "Frecuencia": "2 Días", "Detalles": "Jabones, Desengrasantes, Cloro, Papel Higiénico, Fibras, Trapos."},
            {"Grupo": "F", "Categoría": "Comida y Vitrina", "Frecuencia": "2 Días", "Detalles": "Panadería, Postres, Galletas, Alimentos preparados."},
            {"Grupo": "G", "Categoría": "Retail / Otros", "Frecuencia": "2 Días", "Detalles": "Café p/venta en grano, Filtros de máquina, Papelería, Merch, Utensilios."}
        ]
        df_guia = pd.DataFrame(data_grupos).set_index("Grupo")
        st.table(df_guia)
    
    # --- ZONA DE ADMINISTRADOR ---
    if st.session_state.user_role == "admin":
        st.divider()
        st.write("**🛠️ Administración Avanzada:**")
        
        if st.button("🔒 Corte de Mes", use_container_width=True):
            cambiar_pagina("CorteMes")
        
        st.divider()
        with st.expander("👤 Gestión de Accesos"):
            st.write("**Agregar / Actualizar Barista**")
            n_nombre = st.text_input("Nombre de Usuario:")
            n_clave = st.text_input("Clave de Acceso:")
            n_rol = st.selectbox("Nivel de Permisos:", ["barista", "admin"])
            
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

    # --- CATÁLOGO ---
    if st.session_state.auth_status:
        df_raw_sb, _ = cargar_datos_integrales()
        
        st.divider()
        st.subheader("🛠️ Gestión del Catálogo")
        op_cat = st.radio("Acción:", ["Añadir Insumo", "Editar Insumo"])
        
        ws_ins_cat, err_cat = safe_worksheet(sh, "Insumos")
        
        if op_cat == "Añadir Insumo":
            with st.form("f_add", clear_on_submit=True):
                u = st.selectbox("Unidad", UNIDADES)
                n = st.text_input("Nombre del Insumo")
                m = st.text_input("Marca")
                p = st.text_input("Proveedor")
                g = st.selectbox("Grupo", GRUPOS)
                uc = st.text_input("Presentación de Compra")
                um = st.selectbox("Unidad de Medida", UNIDADES_MED)
                sm = st.number_input("Stock Mínimo", min_value=0.0)
                
                if st.form_submit_button("✨ Crear Insumo"):
                    if err_cat:
                        st.error(err_cat)
                    elif not n.strip():
                        st.error("El nombre es obligatorio.")
                    else:
                        try:
                            nueva_fila = [u, n.strip(), m, p, g, "", uc, um, "", "", "", sm]
                            ws_ins_cat.append_row(nueva_fila)
                            st.cache_data.clear()
                            st.success(f"Insumo '{n}' integrado.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al conectar con la base: {e}")
                            
        else:
            ins_nombres = df_raw_sb["Nombre del Insumo"].dropna().unique().tolist() if not df_raw_sb.empty else []
            if ins_nombres:
                ins_edit = st.selectbox("Seleccionar Insumo a Editar:", ins_nombres)
                d = df_raw_sb[df_raw_sb["Nombre del Insumo"] == ins_edit].iloc[0]
                
                with st.form("f_edit"):
                    e_u = st.selectbox("Unidad", UNIDADES, index=UNIDADES.index(d.get("Unidad de Negocio")) if d.get("Unidad de Negocio") in UNIDADES else 0)
                    e_n = st.text_input("Nombre", value=str(d.get("Nombre del Insumo", "")))
                    e_m = st.text_input("Marca", value=str(d.get("Marca", "")))
                    e_p = st.text_input("Proveedor", value=str(d.get("Proveedor", "")))
                    g_val = str(d.get("Grupo", "A"))
                    e_g = st.selectbox("Grupo", GRUPOS, index=GRUPOS.index(g_val) if g_val in GRUPOS else 0)
                    e_uc = st.text_input("Presentación Compra", value=str(d.get("Presentación de Compra", "")))
                    u_val = str(d.get("Unidad de Medida", "pz")).lower()
                    e_um = st.selectbox("Medida", UNIDADES_MED, index=UNIDADES_MED.index(u_val) if u_val in UNIDADES_MED else 0)
                    e_sm = st.number_input("Stock Mínimo", value=limpiar_valor(d.get("Stock Mínimo", 0)))
                    
                    if st.form_submit_button("💾 Actualizar Insumo"):
                        if err_cat:
                            st.error(err_cat)
                        else:
                            try:
                                idx = int(d.get("Sheet_Row_Num", 0))
                                if idx < 2:
                                    raise ValueError("Número de fila inválido.")
                                    
                                fila_act = [[e_u, e_n.strip(), e_m, e_p, e_g, "", e_uc, e_um, "", "", "", e_sm]]
                                ws_ins_cat.update(range_name=f"A{idx}:L{idx}", values=fila_act)
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
df_raw, df_historial = cargar_datos_integrales()

if pagina == "Dashboard":
    st.title("📊 Dashboard Operativo")
    
    ahora = datetime.now()
    dias_faltantes = calendar.monthrange(ahora.year, ahora.month)[1] - ahora.day
    
    if dias_faltantes <= 4:
        st.info(f"⏳ **Recordatorio de Administración:** Estamos a {dias_faltantes} días del fin de mes. No olvides ejecutar el **Corte de Mes**.")

    df_actual = obtener_ultimo_inventario(df_historial)
    
    if not df_actual.empty:
        crit = df_actual[df_actual['Necesita Compra'] == True]
        
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("🛒 Pendientes Noble", len(crit[crit['Unidad de Negocio']=="Noble"]))
        with c2:
            st.metric("🛒 Pendientes Coffee Station", len(crit[crit['Unidad de Negocio']=="Coffee Station"]))
        with c3:
            max_dt = df_actual['Fecha de Inventario'].max()
            st.metric("🕒 Último Movimiento", max_dt.strftime("%d/%m %H:%M") if pd.notna(max_dt) else "-")
        
        st.divider()
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏢 Faltantes: Noble")
            ins_n = crit[crit['Unidad de Negocio']=="Noble"]
            if not ins_n.empty:
                for _, r in ins_n.iterrows():
                    st.error(f"**{r['Nombre del Insumo']}** (Stock: {r['Stock Neto Calculado']} / Mín: {r['Stock Mínimo']})")
            else:
                st.success("Operación cubierta.")
                
        with col2:
            st.subheader("☕ Faltantes: Coffee Station")
            ins_cs = crit[crit['Unidad de Negocio']=="Coffee Station"]
            if not ins_cs.empty:
                for _, r in ins_cs.iterrows():
                    st.error(f"**{r['Nombre del Insumo']}** (Stock: {r['Stock Neto Calculado']} / Mín: {r['Stock Mínimo']})")
            else:
                st.success("Operación cubierta.")
        
        st.divider()
        st.subheader("🕒 Actividad Reciente (Logs)")
        df_log_display = df_historial.copy()
        df_log_display['Fecha de Inventario'] = df_log_display['Fecha de Inventario'].combine_first(df_log_display['Espacio_H'])
        logs_mostrar = df_log_display.dropna(subset=['Fecha de Inventario']).sort_values('Fecha de Inventario', ascending=False)
        st.dataframe(logs_mostrar[['Fecha de Inventario', 'Responsable', 'Unidad de Negocio', 'Nombre del Insumo', 'Stock Neto', 'Necesita Compra', 'Comentarios']].head(15), use_container_width=True)
    else:
        st.info("Sin datos históricos. Ejecuta el primer conteo de inventario.")

elif pagina == "Inventario":
    st.title("📝 Capturar inventario")
    
    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida. Por favor ingresa tu Clave en el menú lateral.")
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
            "👤 Responsable", 
            responsables, 
            index=resp_idx, 
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
    busqueda_inv = st.text_input("🔍 Buscar insumo específico para contar:", placeholder="Escribe el nombre del insumo...")

    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    df_f = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"]).reset_index(drop=True)
    
    if busqueda_inv:
        df_f = df_f[df_f["Nombre del Insumo"].astype(str).str.contains(busqueda_inv, case=False, na=False)]
    
    if not df_f.empty:
        regs = {}
        h1, h2, h3, ht, h4, h5, h6, h7 = st.columns([2.0, 0.8, 0.8, 0.8, 0.8, 0.8, 1.0, 1.8])
        with h1: st.write("**Insumo / Ref**")
        with h2: st.write("**Almacén**")
        with h3: st.write("**Barra**")
        with ht: st.write("**Tara**")
        with h4: st.write("**Medida**")
        with h5: st.write("**Neto**")
        with h6: st.write("**¿Pedir?**")
        with h7: st.write("**Comentarios**")
        st.divider()

        for _, row in df_f.iterrows():
            nom = str(row.get('Nombre del Insumo', ''))
            safe_nom = normalizar_nombre(nom).replace(" ", "_")
            v_prev = 0.0
            
            match = df_actual[df_actual['Nombre del Insumo'] == nom]
            if not match.empty:
                v_prev = match.iloc[0]['Stock Neto Calculado']
                
            v_min = limpiar_valor(row.get('Stock Mínimo', 0))

            with st.container():
                c1, c2, c3, ct, c4, c5, c6, c7 = st.columns([2.0, 0.8, 0.8, 0.8, 0.8, 0.8, 1.0, 1.8])
                with c1:
                    st.write(f"**{nom}**")
                    st.caption(f"Marca: {row.get('Marca','-')} | Prov: {row.get('Proveedor','-')}")
                    diff = v_prev - v_min
                    color = "green" if diff >= 0 else "red"
                    st.markdown(f"<small>Ant: {v_prev} | Mín: {v_min} (<span style='color:{color}'>{diff:+.1f}</span>)</small>", unsafe_allow_html=True)
                
                with c2:
                    v_a_in = st.number_input(
                        "Alm", 
                        value=None, 
                        min_value=0.0, 
                        step=1.0, 
                        key=f"a_{safe_nom}", 
                        label_visibility="collapsed",
                        placeholder="0"
                    )
                with c3:
                    v_b_in = st.number_input(
                        "Bar", 
                        value=None, 
                        min_value=0.0, 
                        step=1.0, 
                        key=f"b_{safe_nom}", 
                        label_visibility="collapsed",
                        placeholder="0"
                    )
                with ct:
                    v_t_in = st.number_input(
                        "Tara", 
                        value=None, 
                        min_value=0.0, 
                        step=1.0, 
                        key=f"t_{safe_nom}", 
                        label_visibility="collapsed",
                        placeholder="0"
                    )
                    
                v_a = v_a_in if v_a_in is not None else 0.0
                v_b = v_b_in if v_b_in is not None else 0.0
                v_t = v_t_in if v_t_in is not None else 0.0

                with c4:
                    u_act = str(row.get("Unidad de Medida","pz")).lower()
                    v_u = st.selectbox(
                        "U", 
                        UNIDADES_MED, 
                        index=UNIDADES_MED.index(u_act) if u_act in UNIDADES_MED else 0, 
                        key=f"u_{safe_nom}", 
                        label_visibility="collapsed"
                    )
                
                v_n = max(0.0, (v_a + v_b) - v_t)
                
                adj_a, adj_b = v_a, v_b
                if v_t > 0:
                    if adj_a >= v_t:
                        adj_a -= v_t
                    elif adj_b >= v_t:
                        adj_b -= v_t
                    else:
                        adj_a = max(0.0, adj_a - v_t/2)
                        adj_b = max(0.0, adj_b - v_t/2)

                with c5:
                    st.write(f"**{v_n:.1f}**")
                
                with c6:
                    p_key = f"p_{safe_nom}"
                    manual_key = f"m_{safe_nom}"
                    last_math_k = f"lm_{safe_nom}"
                    
                    if manual_key not in st.session_state:
                        st.session_state[manual_key] = None
                    if last_math_k not in st.session_state:
                        st.session_state[last_math_k] = None
                    
                    math_val = bool(v_n < v_min)
                    
                    if st.session_state[last_math_k] != math_val:
                        st.session_state[manual_key] = None
                        st.session_state[last_math_k] = math_val
                        
                    def make_cb(k, mk):
                        def cb():
                            st.session_state[mk] = st.session_state[k]
                        return cb
                        
                    if st.session_state[manual_key] is None:
                        st.session_state[p_key] = math_val
                        
                    v_p = st.toggle("🛒", key=p_key, on_change=make_cb(p_key, manual_key)) 
                
                with c7:
                    v_c = st.text_input(
                        "Nota...", 
                        key=f"c_{safe_nom}", 
                        label_visibility="collapsed", 
                        placeholder="Opcional"
                    )
                
                tara_str = f" [Tara desc.: {v_t}]" if v_t > 0 else ""
                
                regs[nom] = {
                    "a": adj_a, 
                    "b": adj_b, 
                    "n": v_n, 
                    "u": v_u, 
                    "p": v_p, 
                    "c": f"{v_c}{tara_str}".strip(), 
                    "row": row
                }
            st.divider()

        proc_inv = st.session_state.get("_procesando_inventario", False)
        btn_inv = st.button("📥 PROCESAR INVENTARIO", use_container_width=True, type="primary", disabled=proc_inv)
        
        if btn_inv and not proc_inv:
            st.session_state["_procesando_inventario"] = True
            ws_his, err = safe_worksheet(sh, "Historial")
            
            if err:
                st.error(err)
                st.session_state["_procesando_inventario"] = False
            else:
                fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                filas = []
                for n, info in regs.items():
                    dm = info["row"]
                    filas.append([
                        u_sel, n, dm.get("Marca", ""), dm.get("Proveedor", ""), dm.get("Grupo", ""), "", 
                        dm.get("Presentación de Compra", ""), info["u"], info["a"], info["b"], 
                        info["n"], dm.get("Stock Mínimo", 0), "TRUE" if info["p"] else "FALSE", r_sel, fh, info["c"]
                    ])
                
                ok, msg = append_rows_con_retry(ws_his, filas)
                st.session_state["_procesando_inventario"] = False
                
                if ok:
                    st.cache_data.clear()
                    st.success("¡Transacción exitosa! Inventario actualizado.")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(msg)
                    elif pagina == "Ingresos":
    st.title("📥 Entrada de compras")
    
    if not st.session_state.auth_status:
        st.error("🔒 Autenticación requerida.")
        st.stop()

    st.info("Ingresa insumos recibidos. Se sumarán a tu último corte de Almacén.")
    
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
            "👤 Responsable:", 
            responsables, 
            index=resp_idx, 
            disabled=(st.session_state.user_role != "admin")
        )

    df_u = (
        df_raw[df_raw["Unidad de Negocio"] == u_sel] 
        if not df_raw.empty else pd.DataFrame()
    )
    
    if not df_u.empty:
        df_actual = obtener_ultimo_inventario(df_historial, u_sel)
        nombres_insumos = df_u["Nombre del Insumo"].dropna().unique().tolist()
        
        st.divider()
        modo_bulk = st.toggle("🚀 Activar Ingreso Masivo Rápido (Bulk)")
        
        if modo_bulk:
            st.subheader("Carga Bulk")
            bulk_data = []
            
            for _, r in df_u.iterrows():
                nom = r['Nombre del Insumo']
                v_a_prev, v_b_prev = 0.0, 0.0
                
                if not df_actual.empty:
                    m = df_actual[df_actual['Nombre del Insumo'] == nom]
                    if not m.empty:
                        v_a_prev = m.iloc[0]['Alm']
                        v_b_prev = m.iloc[0]['Barra']
                        
                bulk_data.append({
                    "Insumo": nom, 
                    "Stock Alm": v_a_prev, 
                    "Stock Barra": v_b_prev, 
                    "+ Ingreso": None, 
                    "Tara (Contenedor)": None, 
                    "row_data": r
                })
                
            df_edit = pd.DataFrame(bulk_data)
            edited_df = st.data_editor(
                df_edit[['Insumo', 'Stock Alm', 'Stock Barra', '+ Ingreso', 'Tara (Contenedor)']], 
                hide_index=True, 
                use_container_width=True, 
                disabled=['Insumo', 'Stock Alm', 'Stock Barra']
            )
            
            proc_bulk = st.session_state.get("_procesando_bulk", False)
            btn_bulk = st.button("📦 EJECUTAR INGRESO BULK", type="primary", disabled=proc_bulk)
            
            if btn_bulk and not proc_bulk:
                st.session_state["_procesando_bulk"] = True
                ws_his, err = safe_worksheet(sh, "Historial")
                
                if err:
                    st.error(err)
                    st.session_state["_procesando_bulk"] = False
                else:
                    fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    filas_bulk = []
                    
                    for _, r_ed in edited_df.iterrows():
                        ing_val = r_ed['+ Ingreso']
                        tara_val = r_ed.get('Tara (Contenedor)')
                        
                        ingreso = limpiar_valor(ing_val) if ing_val is not None and str(ing_val).strip() != "" else 0.0
                        tara = limpiar_valor(tara_val) if tara_val is not None and str(tara_val).strip() != "" else 0.0
                        neto_ingresado = max(0.0, ingreso - tara)
                        
                        if neto_ingresado > 0:
                            nom = r_ed['Insumo']
                            orig = next((x for x in bulk_data if x["Insumo"] == nom), None)
                            if not orig:
                                continue
                                
                            row_ins = orig["row_data"]
                            v_min = limpiar_valor(row_ins.get("Stock Mínimo", 0))
                            
                            nuevo_a = orig["Stock Alm"] + neto_ingresado
                            nuevo_n = nuevo_a + orig["Stock Barra"]
                            tara_str = f" [Tara desc.: {tara}]" if tara > 0 else ""
                            
                            filas_bulk.append([
                                u_sel, nom, row_ins.get("Marca", ""), row_ins.get("Proveedor", ""), row_ins.get("Grupo", ""), fh, 
                                row_ins.get("Presentación de Compra", ""), row_ins.get("Unidad de Medida", "pz"), 
                                nuevo_a, orig["Stock Barra"], nuevo_n, v_min, "TRUE" if nuevo_n < v_min else "FALSE", r_sel, "", f"Ingreso Bulk{tara_str}"
                            ])
                    
                    if filas_bulk:
                        ok, msg = append_rows_con_retry(ws_his, filas_bulk)
                        st.session_state["_procesando_bulk"] = False
                        
                        if ok:
                            st.cache_data.clear()
                            st.success(f"Ingreso masivo de {len(filas_bulk)} referencias registrado con éxito.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)
                    else:
                        st.session_state["_procesando_bulk"] = False
                        st.warning("No registraste cantidades mayores a 0 en la cuadrícula.")
        
        else:
            insumos_llegados = st.multiselect("🔍 Selecciona insumos individuales que llegaron:", sorted(nombres_insumos))
            if insumos_llegados:
                regs_ingreso = {}
                st.divider()
                
                h1, h2, h3, ht, h4 = st.columns([3, 2, 2, 1.5, 2])
                with h1: st.write("**Insumo**")
                with h2: st.write("**Stock Anterior (Alm+Bar)**")
                with h3: st.write("**+ Ingreso**")
                with ht: st.write("**- Tara**")
                with h4: st.write("**= Nuevo Stock**")
                st.divider()

                for i, nom in enumerate(insumos_llegados):
                    row_ins = df_u[df_u["Nombre del Insumo"] == nom].iloc[0]
                    v_a_prev, v_b_prev = 0.0, 0.0
                    
                    if not df_actual.empty:
                        m = df_actual[df_actual['Nombre del Insumo'] == nom]
                        if not m.empty:
                            v_a_prev = m.iloc[0]['Alm']
                            v_b_prev = m.iloc[0]['Barra']
                            
                    v_min = limpiar_valor(row_ins.get('Stock Mínimo', 0))
                    
                    with st.container():
                        c1, c2, c3, ct, c4 = st.columns([3, 2, 2, 1.5, 2])
                        with c1: 
                            st.write(f"**{nom}**")
                            st.caption(f"Marca: {row_ins.get('Marca','-')} | Prov: {row_ins.get('Proveedor', '-')}")
                        with c2: 
                            st.write(f"Almacén: {v_a_prev} | Barra: {v_b_prev}")
                            st.write(f"**Total Ant: {v_a_prev + v_b_prev}**")
                        with c3:
                            v_in = st.number_input("Ingreso a Almacén", value=None, min_value=0.0, step=1.0, key=f"ing_{i}", label_visibility="collapsed", placeholder="0")
                        with ct:
                            v_ta = st.number_input("Tara", value=None, min_value=0.0, step=1.0, key=f"tara_{i}", label_visibility="collapsed", placeholder="0")
                        with c4:
                            calc_in = v_in if v_in is not None else 0.0
                            calc_ta = v_ta if v_ta is not None else 0.0
                            neto_ingreso = max(0.0, calc_in - calc_ta)
                            nuevo_alm = v_a_prev + neto_ingreso
                            nuevo_neto = nuevo_alm + v_b_prev
                            st.success(f"**{nuevo_neto:.1f}**")
                            
                        regs_ingreso[nom] = {
                            "nuevo_a": nuevo_alm, 
                            "b": v_b_prev, 
                            "nuevo_n": nuevo_neto, 
                            "row": row_ins, 
                            "min": v_min, 
                            "tara_usada": calc_ta
                        }
                    st.divider()

                proc_ing = st.session_state.get("_procesando_ingreso", False)
                btn_ing = st.button("📦 EJECUTAR INGRESO", use_container_width=True, type="primary", disabled=proc_ing)
                
                if btn_ing and not proc_ing:
                    st.session_state["_procesando_ingreso"] = True
                    ws_his, err = safe_worksheet(sh, "Historial")
                    
                    if err:
                        st.error(err)
                        st.session_state["_procesando_ingreso"] = False
                    else:
                        fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        filas = []
                        
                        for n, info in regs_ingreso.items():
                            dm = info["row"]
                            tara_str = f" [Tara desc.: {info['tara_usada']}]" if info['tara_usada'] > 0 else ""
                            filas.append([
                                u_sel, n, dm.get("Marca", ""), dm.get("Proveedor", ""), dm.get("Grupo", ""), fh, 
                                dm.get("Presentación de Compra", ""), dm.get("Unidad de Medida", "pz"), 
                                info["nuevo_a"], info["b"], info["nuevo_n"], info["min"], "TRUE" if info["nuevo_n"] < info["min"] else "FALSE", r_sel, "", f"Ingreso Individual{tara_str}"
                            ])
                            
                        ok, msg = append_rows_con_retry(ws_his, filas)
                        st.session_state["_procesando_ingreso"] = False
                        
                        if ok:
                            st.cache_data.clear()
                            st.success("Ingreso registrado de manera exitosa.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)

elif pagina == "Consulta":
    st.title("📦 Consulta de Stock")
    u_sel = st.selectbox("Unidad", UNIDADES)
    df_act = obtener_ultimo_inventario(df_historial, u_sel)
    
    if not df_act.empty:
        st.dataframe(
            df_act[["Grupo_H", "Nombre del Insumo", "Alm", "Barra", "Stock Neto", "Stock Mínimo", "Necesita Compra"]], 
            use_container_width=True, 
            hide_index=True
        )

elif pagina == "Impresion":
    st.title("🖨️ Ticket de Conteo (58mm)")
    u_sel = st.selectbox("Sucursal:", UNIDADES)
    df_u = (
        df_raw[df_raw["Unidad de Negocio"] == u_sel] 
        if not df_raw.empty else pd.DataFrame()
    )
    grps = (
        sorted(df_u["Grupo"].dropna().unique().tolist()) 
        if not df_u.empty and "Grupo" in df_u.columns else []
    )
    g_sel = st.multiselect("Filtrar por Grupos:", grps)
    
    if g_sel and not df_u.empty:
        df_p = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"])
        t = f"*** CONTEO {u_sel.upper()} ***\n"
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
    u_sel = st.radio("Generar orden para:", UNIDADES, horizontal=True)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    
    if not df_actual.empty:
        com = df_actual[df_actual['Necesita Compra'] == True]
        if not com.empty:
            t = f"*** COMPRAS {u_sel.upper()} ***\n"
            t += f"Fecha: {datetime.now().strftime('%d/%m/%Y')}\n"
            t += "-" * 22 + "\n"
            
            for _, r in com.iterrows():
                t += f"• {str(r['Nombre del Insumo'])[:20]}\n"
                t += f"  Stock: {r['Stock Neto Calculado']} / Min: {r['Stock Mínimo']}\n"
                t += "-" * 22 + "\n"
                
            st.info("Copia el texto para imprimir o enviar a la impresora térmica.")
            st.code(t, language=None)
        else:
            st.success("No se han disparado alertas de reabastecimiento.")
    else:
        st.info("Sin registros suficientes para armar la logística de compra.")

elif pagina == "ReporteStock":
    st.title("📦 Reporte de Stock (58mm)")
    u_sel = st.radio("Generar reporte para:", UNIDADES, horizontal=True)
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    
    if not df_actual.empty:
        t = f"*** INVENTARIO {u_sel.upper()} ***\n"
        t += f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
        t += "-" * 22 + "\n"
        
        df_rep = df_actual.sort_values(["Grupo_H", "Nombre del Insumo"])
        grupo_actual = ""
        
        for _, r in df_rep.iterrows():
            grupo = str(r['Grupo_H'])
            if grupo != grupo_actual:
                t += f"\n>> GRUPO {grupo} <<\n"
                grupo_actual = grupo
            t += f"{str(r['Nombre del Insumo'])[:20]}\n"
            t += f" Alm:{r['Alm']} Bar:{r['Barra']} Total:{r['Stock Neto Calculado']}\n"
            
        t += "-" * 22 + "\n"
        st.info("Copia el texto para imprimir o enviar a la impresora térmica.")
        st.code(t, language=None)
    else:
        st.warning("No hay registros en la base de datos para generar el reporte.")

elif pagina == "CorteMes":
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
                    raise ValueError("No hay datos históricos para cerrar.")

                st.write("2/4 — Escribiendo Backup de seguridad (Archivo_Historial)...")
                fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                filas_corte = []
                
                for _, r in df_corte.iterrows():
                    filas_corte.append([
                        r.get("Unidad de Negocio", ""),
                        r.get("Nombre del Insumo", ""),
                        r.get("Marca_H", ""),
                        r.get("Proveedor_H", ""),
                        r.get("Grupo_H", ""),
                        "",
                        r.get("Pres_Compra_H", ""),
                        r.get("Unidad_Medida_H", ""),
                        r.get("Alm", 0),
                        r.get("Barra", 0),
                        r.get("Stock Neto Calculado", 0),
                        r.get("Stock Mínimo", 0),
                        "TRUE" if r.get("Necesita Compra") else "FALSE",
                        "SISTEMA-CIERRE",
                        fh,
                        "Corte consolidado",
                    ])

                ws_his, err_his = safe_worksheet(sh, "Historial")
                if err_his:
                    raise Exception(err_his)
                    
                datos_hist = ws_his.get_all_values()
                if len(datos_hist) > 1:
                    ws_arc, err_arc = safe_worksheet(sh, "Archivo_Historial")
                    if ws_arc is None:
                        ws_arc = sh.add_worksheet(title="Archivo_Historial", rows="1000", cols="20")
                        ws_arc.append_row(datos_hist[0])
                    ws_arc.append_rows(datos_hist[1:])

                st.write("3/4 — Consolidando nueva base maestra (Cierres)...")
                ws_cie, _ = safe_worksheet(sh, "Cierres")
                if ws_cie is None:
                    ws_cie = sh.add_worksheet(title="Cierres", rows="1000", cols="20")
                ws_cie.clear()
                ws_cie.append_row(COLS_HISTORIAL)
                ws_cie.append_rows(filas_corte)

                st.write("4/4 — Reiniciando Historial...")
                ws_his.clear()
                ws_his.append_row(COLS_HISTORIAL)

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
                    "Revisa los logs antes de intentar nuevamente."
                )
