import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime

# CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Noble Inventario", page_icon="☕", layout="wide")

# 1. CONEXIÓN A GOOGLE SHEETS
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

# 2. GESTIÓN DE RESPONSABLES (SIDEBAR)
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

# 3. CARGA DE DATOS
df_raw = pd.DataFrame(insumos_sheet.get_all_records())

st.title("☕ Noble & Coffee Station: Inventario")

# 4. FILTROS PRINCIPALES
col_u, col_r, col_g = st.columns(3)

with col_u:
    # Filtro por Unidad de Negocio (Columna A)
    unidades_negocio = sorted(df_raw["Unidad de Negocio"].unique().tolist())
    unidad_sel = st.selectbox("🏢 Unidad de Negocio", unidades_negocio)

with col_r:
    responsable = st.selectbox("👤 Responsable", st.session_state.responsables)

# Filtrar primero por Unidad de Negocio para obtener los grupos correctos
df_unidad = df_raw[df_raw["Unidad de Negocio"] == unidad_sel]

with col_g:
    grupos_disponibles = sorted(df_unidad["Grupo"].unique().tolist())
    grupo_sel = st.selectbox("📂 Grupo", grupos_disponibles)

st.divider()

# 5. DESPLIEGUE FILTRADO
df_final = df_unidad[df_unidad["Grupo"] == grupo_sel].reset_index(drop=True)

if not df_final.empty:
    st.subheader(f"Inventario: {unidad_sel} - Grupo {grupo_sel}")
    
    nuevos_registros = {}

    # Encabezados
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

    # 6. BOTÓN DE REGISTRO
    if st.button(f"📥 Registrar Todo en {unidad_sel}", use_container_width=True):
        fecha_hoy = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filas_para_excel = []

        for nombre, info in nuevos_registros.items():
            dm = info["datos_maestros"]
            try:
                stock_min = float(dm.get("Stock Mínimo", 0)) if dm.get("Stock Mínimo") != "" else 0.0
            except: stock_min = 0.0
            
            necesita_compra = "TRUE" if info["neto"] <= stock_min else "FALSE"

            fila = [
                unidad_sel,                                 # A (Unidad de Negocio)
                nombre,                                     # B
                str(dm.get("Marca", "")),                  # C
                str(dm.get("Proveedor", "")),              # D
                str(dm.get("Grupo", "")),                  # E
                str(dm.get("Fecha de Entrada", "")),       # F
                str(dm.get("Presentación de Compra", "")), # G
                info["unidad"],                            # H
                info["almacen"],                           # I
                info["activo"],                            # J
                info["neto"],                              # K
                stock_min,                                 # L
                necesita_compra,                           # M
                responsable,                               # N
                fecha_hoy                                  # O
            ]
            filas_para_excel.append(fila)

        if filas_para_excel:
            historial_sheet.append_rows(filas_para_excel)
            st.success(f"✅ Inventario de {unidad_sel} guardado exitosamente.")
            st.balloons()
else:
    st.warning(f"No hay insumos configurados para {unidad_sel} en este grupo.")
