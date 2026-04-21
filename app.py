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
    # Lista inicial actualizada (José eliminado)
    st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]

with st.sidebar:
    st.header("👥 Gestión de Equipo")
    nuevo_nombre = st.text_input("Nuevo Responsable:")
    if st.button("➕ Agregar al Equipo"):
        if nuevo_nombre and nuevo_nombre not in st.session_state.responsables:
            st.session_state.responsables.append(nuevo_nombre)
            st.success(f"{nuevo_nombre} agregado.")
        
    eliminar_nombre = st.selectbox("Eliminar del Equipo:", st.session_state.responsables)
    if st.button("🗑️ Eliminar Seleccionado"):
        if len(st.session_state.responsables) > 1:
            st.session_state.responsables.remove(eliminar_nombre)
            st.warning(f"{eliminar_nombre} eliminado.")
            st.rerun()
        else:
            st.error("Debe haber al menos un responsable.")

# 3. CARGA DE DATOS
df_maestro = pd.DataFrame(insumos_sheet.get_all_records())

st.title("☕ Noble: Control de Inventario")

# 4. SELECCIÓN DE CONTEXTO
col_a, col_b = st.columns(2)
with col_a:
    responsable = st.selectbox("Responsable", st.session_state.responsables)
with col_b:
    grupos_disponibles = sorted(df_maestro["Grupo"].unique().tolist())
    grupo_sel = st.selectbox("Selecciona Grupo para Inventariar", grupos_disponibles)

st.divider()

# 5. DESPLIEGUE POR GRUPO
df_filtrado = df_maestro[df_maestro["Grupo"] == grupo_sel].reset_index(drop=True)

if not df_filtrado.empty:
    st.subheader(f"Inventario Detallado: Grupo {grupo_sel}")
    
    nuevos_registros = {}

    # Encabezados
    h1, h2, h3, h4, h5 = st.columns([3, 1.5, 1.5, 1.5, 1.5])
    with h1: st.write("**Insumo / Marca**")
    with h2: st.write("**Almacén**")
    with h3: st.write("**Activo (Barra)**")
    with h4: st.write("**Unidad**")
    with h5: st.write("**Neto Total**")
    st.divider()

    for index, row in df_filtrado.iterrows():
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
            unidad_excel = str(row.get("Unidad de Medida", "pz")).lower()
            idx_unidad = unidades.index(unidad_excel) if unidad_excel in unidades else 0
            unidad_sel = st.selectbox("Medida", unidades, index=idx_unidad, key=f"uni_{index}")
        with c5:
            neto = almacen + activo
            st.metric("Total", f"{neto:.1f}")
            
        nuevos_registros[nombre] = {
            "almacen": almacen,
            "activo": activo,
            "neto": neto,
            "unidad": unidad_sel,
            "datos_maestros": row
        }
        st.write("---")

    # 6. BOTÓN DE REGISTRO
    if st.button(f"📥 Guardar Todo el Grupo {grupo_sel}", use_container_width=True):
        fecha_hoy = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filas_para_excel = []

        for nombre, info in nuevos_registros.items():
            dm = info["datos_maestros"]
            try:
                stock_min = float(dm.get("Stock Mínimo", 0)) if dm.get("Stock Mínimo") != "" else 0.0
            except:
                stock_min = 0.0
            
            necesita_compra = "TRUE" if info["neto"] <= stock_min else "FALSE"

            fila = [
                str(dm.get("Unidad de Negocio", "Noble")), 
                nombre,                                     
                str(dm.get("Marca", "")),                  
                str(dm.get("Proveedor", "")),              
                str(dm.get("Grupo", "")),                  
                str(dm.get("Fecha de Entrada", "")),       
                str(dm.get("Presentación de Compra", "")), 
                info["unidad"],                            
                info["almacen"],                           
                info["activo"],                            
                info["neto"],                              
                stock_min,                                 
                necesita_compra,                           
                responsable,                               
                fecha_hoy                                  
            ]
            filas_para_excel.append(fila)

        if filas_para_excel:
            historial_sheet.append_rows(filas_para_excel)
            st.success(f"✅ Registrado por {responsable}.")
            st.balloons()
else:
    st.warning("Selecciona un grupo para ver los insumos.")
