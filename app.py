import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime

# CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Noble Inventario", page_icon="☕", layout="wide")

# 1. CONEXIÓN A GOOGLE SHEETS
@st.cache_resource
def conectar_google_sheets():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
    client = gspread.authorize(creds)
    return client.open_by_key("1VZV81p-JqoaRPzMzsRurF6wntVefyaN5ozs3RJe6uJs")

try:
    sh = conectar_google_sheets()
    insumos_sheet = sh.worksheet("Insumos")
    historial_sheet = sh.worksheet("Historial")
except Exception as e:
    st.error("Error de conexión. Revisa los Secrets.")
    st.stop()

# 2. GESTIÓN DE EQUIPO Y NUEVOS INSUMOS (SIDEBAR)
if "responsables" not in st.session_state:
    st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]

with st.sidebar:
    st.header("👥 Gestión de Equipo")
    nuevo_nombre = st.text_input("Nuevo Responsable:")
    if st.button("➕ Agregar al Equipo"):
        if nuevo_nombre and nuevo_nombre not in st.session_state.responsables:
            st.session_state.responsables.append(nuevo_nombre)
            st.rerun()
    
    eliminar_nombre = st.selectbox("Eliminar del Equipo:", st.session_state.responsables)
    if st.button("🗑️ Eliminar Seleccionado"):
        if len(st.session_state.responsables) > 1:
            st.session_state.responsables.remove(eliminar_nombre)
            st.rerun()

    st.divider()
    st.header("📦 Añadir Nuevo Insumo")
    with st.form("form_nuevo_insumo"):
        n_unidad = st.selectbox("Unidad de Negocio", ["Noble", "Coffee Station"])
        n_nombre = st.text_input("Nombre del Insumo (Ej: Leche Entera)")
        n_marca = st.text_input("Marca")
        n_grupo = st.selectbox("Grupo", ["A", "B", "C", "D", "E", "F"])
        n_medida = st.selectbox("Unidad de Medida", ["pz", "ml", "gr", "%", "kg", "lt"])
        n_minimo = st.number_input("Stock Mínimo", min_value=0.0, step=1.0)
        
        btn_crear = st.form_submit_button("✨ Crear Insumo")
        
        if btn_crear:
            if n_nombre:
                # Crear fila para la pestaña 'Insumos' (A-L según tu estructura)
                # UnidadNegocio, Nombre, Marca, Proveedor, Grupo, FechaEntrada, PresenCompra, UnidadMedida, ..., StockMinimo
                nueva_fila_insumo = [
                    n_unidad, n_nombre, n_marca, "", n_grupo, 
                    "", "", n_medida, "", "", "", n_minimo
                ]
                insumos_sheet.append_row(nueva_fila_insumo)
                st.success(f"Insumo '{n_nombre}' agregado a {n_unidad}")
                st.cache_data.clear() # Limpiar caché para que aparezca en la lista
                st.rerun()
            else:
                st.error("El nombre del insumo es obligatorio")

# 3. CARGA DE DATOS
@st.cache_data(ttl=60) # Actualiza datos cada minuto o al forzar rerun
def cargar_datos():
    return pd.DataFrame(insumos_sheet.get_all_records())

df_raw = cargar_datos()

st.title("☕ Noble & Coffee Station: Inventario")

# 4. FILTROS PRINCIPALES
col_u, col_r, col_g = st.columns(3)

with col_u:
    unidades_negocio = sorted(df_raw["Unidad de Negocio"].unique().tolist()) if not df_raw.empty else ["Noble", "Coffee Station"]
    unidad_sel = st.selectbox("🏢 Unidad de Negocio", unidades_negocio)

with col_r:
    responsable = st.selectbox("👤 Responsable", st.session_state.responsables)

df_unidad = df_raw[df_raw["Unidad de Negocio"] == unidad_sel]

with col_g:
    grupos_disponibles = sorted(df_unidad["Grupo"].unique().tolist()) if not df_unidad.empty else ["A"]
    grupo_sel = st.selectbox("📂 Grupo", grupos_disponibles)

st.divider()

# 5. DESPLIEGUE FILTRADO
df_final = df_unidad[df_unidad["Grupo"] == grupo_sel].reset_index(drop=True)

if not df_final.empty:
    st.subheader(f"Inventario: {unidad_sel} - Grupo {grupo_sel}")
    nuevos_registros = {}

    h1, h2, h3, h4, h5 = st.columns([3, 1.5, 1.5, 1.5, 1.5])
    with h1: st.write("**Insumo / Marca**")
    with h2: st.write("**Almacén**")
    with h3: st.write("**Activo (Barra)**")
    with h4: st.write("**Unidad**")
    with h5: st.write("**Neto Total**")
    st.divider()

    for index, row in df_final.iterrows():
        nombre = row["Nombre del Insumo"]
        c1, c2, c3, c4, c5 = st.columns([3, 1.5, 1.5, 1.5, 1.5])
        
        with c1:
            st.write(f"**{nombre}**")
            st.caption(f"{row.get('Marca', '')}")
        with c2:
            almacen = st.number_input("Cerrado", min_value=0.0, step=1.0, key=f"alm_{index}")
        with c3:
            activo = st.number_input("Abierto", min_value=0.0, step=0.1, key=f"act_{index}")
        with c4:
            unidades = ["pz", "ml", "gr", "%", "kg", "lt"]
            u_excel = str(row.get("Unidad de Medida", "pz")).lower()
            idx_u = unidades.index(u_excel) if u_excel in unidades else 0
            unidad_sel_item = st.selectbox("Medida", unidades, index=idx_u, key=f"uni_{index}")
        with c5:
            neto = almacen + activo
            st.metric("Total", f"{neto:.1f}")
            
        nuevos_registros[nombre] = {
            "almacen": almacen, "activo": activo, "neto": neto,
            "unidad": unidad_sel_item, "datos_maestros": row
        }
        st.write("---")

    if st.button(f"📥 Registrar Inventario en Historial", use_container_width=True):
        fecha_hoy = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filas_para_excel = []

        for nombre, info in nuevos_registros.items():
            dm = info["datos_maestros"]
            try:
                stock_min = float(dm.get("Stock Mínimo", 0)) if dm.get("Stock Mínimo") != "" else 0.0
            except: stock_min = 0.0
            
            necesita_compra = "TRUE" if info["neto"] <= stock_min else "FALSE"

            fila = [
                unidad_sel, nombre, str(dm.get("Marca", "")), str(dm.get("Proveedor", "")),
                str(dm.get("Grupo", "")), "", "", info["unidad"],
                info["almacen"], info["activo"], info["neto"], stock_min,
                necesita_compra, responsable, fecha_hoy
            ]
            filas_para_excel.append(fila)

        if filas_para_excel:
            historial_sheet.append_rows(filas_para_excel)
            st.success("✅ Datos enviados correctamente.")
            st.balloons()
else:
    st.info("No hay insumos en este grupo. Puedes agregar uno nuevo desde la barra lateral.")
