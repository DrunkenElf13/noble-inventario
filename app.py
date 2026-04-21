import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime

# CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Noble & Coffee Station", page_icon="☕", layout="wide")

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

# 2. CARGA DE DATOS
@st.cache_data(ttl=60)
def cargar_datos_maestros():
    return pd.DataFrame(insumos_sheet.get_all_records())

@st.cache_data(ttl=60)
def cargar_historial():
    return pd.DataFrame(historial_sheet.get_all_records())

df_raw = cargar_datos_maestros()
df_historial = cargar_historial()

# 3. LÓGICA DE NAVEGACIÓN
if "pagina" not in st.session_state:
    st.session_state.pagina = "Portada"

def cambiar_pagina(nombre):
    st.session_state.pagina = nombre

# --- SIDEBAR (GESTIÓN) ---
with st.sidebar:
    st.title("⚙️ Configuración")
    if st.button("🏠 Volver a Portada"): cambiar_pagina("Portada")
    if st.button("📝 Ir a Inventario"): cambiar_pagina("Inventario")
    
    st.divider()
    if "responsables" not in st.session_state:
        st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]
    
    st.subheader("👥 Equipo")
    n_nom = st.text_input("Nuevo Barista:")
    if st.button("➕ Agregar"):
        if n_nom: st.session_state.responsables.append(n_nom); st.rerun()
    
    st.divider()
    st.subheader("📦 Nuevo Insumo")
    with st.form("nuevo_insumo"):
        u = st.selectbox("Unidad", ["Noble", "Coffee Station"])
        n = st.text_input("Nombre")
        g = st.selectbox("Grupo", ["A", "B", "C", "D", "E", "F"])
        m = st.number_input("Mínimo", min_value=0.0)
        if st.form_submit_button("Crear"):
            insumos_sheet.append_row([u, n, "", "", g, "", "", "pz", "", "", "", m])
            st.cache_data.clear(); st.rerun()

# --- PÁGINA: PORTADA ---
if st.session_state.pagina == "Portada":
    st.title("📊 Resumen Operativo")
    
    # Obtener último estado de cada insumo del historial
    if not df_historial.empty:
        # Limpieza de fechas para ordenar
        df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
        ultimo_estado = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
        
        # Filtrar los que necesitan compra
        por_comprar = ultimo_estado[ultimo_estado['Necesita Compra'].astype(str).str.upper() == "TRUE"]
        
        c1, c2 = st.columns(2)
        with c1:
            st.metric("Noble: Por Comprar", len(por_comprar[por_comprar['Unidad de Negocio'] == "Noble"]))
        with c2:
            st.metric("Coffee Station: Por Comprar", len(por_comprar[por_comprar['Unidad de Negocio'] == "Coffee Station"]))
        
        st.divider()
        st.subheader("⚠️ Insumos Críticos (Bajo el Mínimo)")
        
        sucursal = st.radio("Ver sucursal:", ["Noble", "Coffee Station"], horizontal=True)
        lista_critica = por_comprar[por_comprar['Unidad de Negocio'] == sucursal]
        
        if not lista_critica.empty:
            # Mostrar tabla elegante
            st.dataframe(
                lista_critica[['Nombre del Insumo', 'Grupo', 'Stock Neto', 'Stock Mínimo', 'Responsable']],
                use_container_width=True, hide_index=True
            )
        else:
            st.success(f"✅ ¡Todo en orden en {sucursal}! No hay faltantes.")
    else:
        st.info("No hay datos históricos suficientes para mostrar el resumen.")

    st.divider()
    if st.button("🚀 COMENZAR INVENTARIO DEL DÍA", use_container_width=True, type="primary"):
        cambiar_pagina("Inventario")
        st.rerun()

# --- PÁGINA: INVENTARIO ---
elif st.session_state.pagina == "Inventario":
    st.title("📝 Registro de Inventario")
    
    col_u, col_r, col_g = st.columns(3)
    with col_u:
        u_neg = sorted(df_raw["Unidad de Negocio"].unique().tolist()) if not df_raw.empty else ["Noble"]
        u_sel = st.selectbox("🏢 Unidad", u_neg)
    with col_r:
        resp = st.selectbox("👤 Responsable", st.session_state.responsables)
    with col_g:
        df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel]
        grupos = sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"]
        g_sel = st.selectbox("📂 Grupo", grupos)

    df_f = df_u[df_u["Grupo"] == g_sel].reset_index(drop=True)

    if not df_f.empty:
        registros = {}
        st.divider()
        for idx, row in df_f.iterrows():
            nombre = row["Nombre del Insumo"]
            c1, c2, c3, c4 = st.columns([3, 1.5, 1.5, 1.5])
            with c1: st.write(f"**{nombre}**"); st.caption(row.get('Marca', ''))
            with c2: alm = st.number_input("Cerrado", min_value=0.0, step=1.0, key=f"a{idx}")
            with c3: act = st.number_input("Abierto", min_value=0.0, step=0.1, key=f"t{idx}")
            with c4:
                neto = alm + act
                st.metric("Neto", f"{neto:.1f}")
            
            registros[nombre] = {"alm": alm, "act": act, "neto": neto, "row": row}
            st.write("---")

        if st.button("📥 Guardar Inventario", use_container_width=True, type="primary"):
            filas = []
            f_hoy = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for nom, info in registros.items():
                dm = info["row"]
                s_min = float(dm.get("Stock Mínimo", 0)) if dm.get("Stock Mínimo") != "" else 0.0
                compra = "TRUE" if info["neto"] <= s_min else "FALSE"
                filas.append([u_sel, nom, dm.get('Marca',''), "", dm.get('Grupo',''), "", "", 
                              dm.get('Unidad de Medida','pz'), info["alm"], info["act"], 
                              info["neto"], s_min, compra, resp, f_hoy])
            
            historial_sheet.append_rows(filas)
            st.success("✅ Inventario guardado."); st.balloons()
            st.cache_data.clear() # Limpiar para actualizar portada
