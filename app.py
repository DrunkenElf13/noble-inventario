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
@st.cache_data(ttl=30)
def cargar_datos_maestros():
    return pd.DataFrame(insumos_sheet.get_all_records())

@st.cache_data(ttl=30)
def cargar_historial():
    return pd.DataFrame(historial_sheet.get_all_records())

df_raw = cargar_datos_maestros()
df_historial = cargar_historial()

# 3. LÓGICA DE NAVEGACIÓN
if "pagina" not in st.session_state:
    st.session_state.pagina = "Portada"

def cambiar_pagina(nombre):
    st.session_state.pagina = nombre
    st.rerun()

# --- SIDEBAR ---
with st.sidebar:
    st.title("⚙️ Operaciones")
    if st.button("📊 RESUMEN CRÍTICO", use_container_width=True): cambiar_pagina("Portada")
    if st.button("📝 REGISTRAR INVENTARIO", use_container_width=True): cambiar_pagina("Inventario")
    
    st.divider()
    if "responsables" not in st.session_state:
        st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]
    
    with st.expander("👤 Equipo"):
        n_nom = st.text_input("Nuevo Barista:")
        if st.button("➕ Agregar"):
            if n_nom: st.session_state.responsables.append(n_nom); st.rerun()

# --- PÁGINA: PORTADA (ENFOQUE EN COLUMNA M) ---
if st.session_state.pagina == "Portada":
    st.title("🚨 Prioridades de Compra")
    st.info("Este resumen se basa en la Columna M (Criterio de Compra) del último registro.")

    if not df_historial.empty:
        df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
        ultimo_estado = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
        
        # Filtro de los que SI necesitan compra
        por_comprar = ultimo_estado[ultimo_estado['Necesita Compra'].astype(str).str.upper() == "TRUE"].copy()
        
        c1, c2 = st.columns(2)
        with c1: st.metric("Noble: Críticos", len(por_comprar[por_comprar['Unidad de Negocio'] == "Noble"]))
        with c2: st.metric("Coffee Station: Críticos", len(por_comprar[por_comprar['Unidad de Negocio'] == "Coffee Station"]))
        
        st.divider()
        sucursal = st.radio("Sucursal a surtir:", ["Noble", "Coffee Station"], horizontal=True)
        lista = por_comprar[por_comprar['Unidad de Negocio'] == sucursal]

        if not lista.empty:
            st.subheader(f"🛒 Lista de Compras Prioritaria - {sucursal}")
            # Estilo de tarjetas para resaltar la importancia
            for _, row in lista.iterrows():
                with st.expander(f"🔴 {row['Nombre del Insumo']} (Stock: {row['Stock Neto']})"):
                    col1, col2, col3 = st.columns(3)
                    col1.write(f"**Marca:** {row.get('Marca','')}")
                    col2.write(f"**Proveedor:** {row.get('Proveedor','')}")
                    col3.write(f"**Mínimo:** {row.get('Stock Mínimo','')}")
                    st.write(f"✍️ *Registrado por {row['Responsable']} el {row['Fecha de Inventario'].strftime('%d/%m %H:%M')}*")
        else:
            st.success(f"✅ ¡Felicidades! No hay insumos marcados para compra en {sucursal}.")
    else:
        st.warning("No hay historial para calcular prioridades.")

    if st.button("🚀 INICIAR NUEVO LEVANTAMIENTO", use_container_width=True, type="primary"): cambiar_pagina("Inventario")

# --- PÁGINA: INVENTARIO (APLICACIÓN DE CRITERIO HUMANO) ---
elif st.session_state.pagina == "Inventario":
    st.title("📝 Levantamiento de Stock")
    
    col_u, col_r, col_g = st.columns(3)
    with col_u: u_sel = st.selectbox("🏢 Unidad", ["Noble", "Coffee Station"])
    with col_r: resp = st.selectbox("👤 Responsable", st.session_state.responsables)
    with col_g:
        df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel]
        grupos = sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"]
        g_sel = st.selectbox("📂 Grupo", grupos)

    df_f = df_u[df_u["Grupo"] == g_sel].reset_index(drop=True)

    if not df_f.empty:
        registros = {}
        st.markdown("### Insumos del Grupo")
        for idx, row in df_f.iterrows():
            with st.container():
                c1, c2, c3, c4, c5 = st.columns([2.5, 1, 1, 1, 1.5])
                with c1:
                    st.write(f"**{row['Nombre del Insumo']}**")
                    st.caption(f"{row.get('Marca','')} | {row.get('Presentación de Compra','')}")
                with c2: alm = st.number_input("Almacén", min_value=0.0, step=1.0, key=f"a{idx}")
                with c3: act = st.number_input("Barra", min_value=0.0, step=0.1, key=f"t{idx}")
                with c4:
                    neto = alm + act
                    st.metric("Neto", f"{neto:.1f}")
                with c5:
                    # AQUÍ ESTÁ EL PODER DE LA COLUMNA M:
                    # El sistema sugiere comprar si neto <= mínimo, pero el barista PUEDE cambiarlo
                    s_min = float(row.get("Stock Mínimo", 0) or 0)
                    sugiere_compra = neto <= s_min
                    criterio_humano = st.toggle("¿Pedir?", value=sugiere_compra, key=f"m{idx}", help="Activa esto para marcarlo como prioridad de compra.")
                
                registros[row['Nombre del Insumo']] = {"alm": alm, "act": act, "neto": neto, "compra": criterio_humano, "row": row}
                st.divider()

        if st.button("📥 FINALIZAR REGISTRO", use_container_width=True, type="primary"):
            filas = []
            f_hoy = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for nom, info in registros.items():
                dm = info["row"]
                compra_val = "TRUE" if info["compra"] else "FALSE"
                filas.append([u_sel, nom, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                              dm.get('Presentación de Compra',''), dm.get('Unidad de Medida','pz'), 
                              info["alm"], info["act"], info["neto"], dm.get('Stock Mínimo',0), 
                              compra_val, resp, f_hoy])
            
            historial_sheet.append_rows(filas)
            st.cache_data.clear()
            st.success("✅ Inventario y Prioridades guardadas."); st.balloons()
