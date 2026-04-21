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
    data = insumos_sheet.get_all_records()
    return pd.DataFrame(data)

@st.cache_data(ttl=60)
def cargar_historial():
    data = historial_sheet.get_all_records()
    return pd.DataFrame(data)

df_raw = cargar_datos_maestros()
df_historial = cargar_historial()

# 3. LÓGICA DE NAVEGACIÓN
if "pagina" not in st.session_state:
    st.session_state.pagina = "Portada"

def cambiar_pagina(nombre):
    st.session_state.pagina = nombre
    st.rerun()

# --- SIDEBAR (GESTIÓN) ---
with st.sidebar:
    st.title("⚙️ Panel de Control")
    if st.button("🏠 Resumen / Portada", use_container_width=True): cambiar_pagina("Portada")
    if st.button("📝 Hacer Inventario", use_container_width=True): cambiar_pagina("Inventario")
    
    st.divider()
    if "responsables" not in st.session_state:
        st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]
    
    with st.expander("👤 Gestionar Equipo"):
        n_nom = st.text_input("Nuevo Barista:")
        if st.button("➕ Agregar"):
            if n_nom: st.session_state.responsables.append(n_nom); st.rerun()

    st.divider()
    opcion_insumo = st.radio("Acción de Insumos:", ["Añadir Nuevo", "Editar Existente"])

    if opcion_insumo == "Añadir Nuevo":
        st.subheader("📦 Nuevo Insumo")
        with st.form("nuevo_insumo"):
            u = st.selectbox("Unidad", ["Noble", "Coffee Station"])
            n = st.text_input("Nombre")
            m = st.text_input("Marca")
            p = st.text_input("Proveedor")
            g = st.selectbox("Grupo", ["A", "B", "C", "D", "E", "F"])
            uc = st.text_input("Unidad de Compra (Ej: Caja 12L)")
            um = st.selectbox("Medida de Conteo", ["pz", "ml", "gr", "%", "kg", "lt"])
            s_min = st.number_input("Stock Mínimo", min_value=0.0)
            if st.form_submit_button("✨ Crear"):
                insumos_sheet.append_row([u, n, m, p, g, "", uc, um, "", "", "", s_min])
                st.cache_data.clear(); st.success("Creado"); st.rerun()

    else:
        st.subheader("✏️ Editar Insumo")
        insumo_a_editar = st.selectbox("Selecciona:", df_raw["Nombre del Insumo"].tolist() if not df_raw.empty else [])
        if insumo_a_editar:
            datos_act = df_raw[df_raw["Nombre del Insumo"] == insumo_a_editar].iloc[0]
            with st.form("editar_insumo"):
                e_u = st.selectbox("Unidad", ["Noble", "Coffee Station"], index=0 if datos_act["Unidad de Negocio"] == "Noble" else 1)
                e_n = st.text_input("Nombre", value=str(datos_act["Nombre del Insumo"]))
                e_m = st.text_input("Marca", value=str(datos_act.get("Marca", "")))
                e_p = st.text_input("Proveedor", value=str(datos_act.get("Proveedor", "")))
                e_g = st.selectbox("Grupo", ["A", "B", "C", "D", "E", "F"], index=["A","B","C","D","E","F"].index(datos_act.get("Grupo", "A")))
                e_uc = st.text_input("Unidad de Compra", value=str(datos_act.get("Presentación de Compra", "")))
                
                # FIX PARA EL ERROR: Validar unidad de medida
                lista_unidades = ["pz", "ml", "gr", "%", "kg", "lt"]
                u_valor = str(datos_act.get("Unidad de Medida", "pz")).lower()
                u_idx = lista_unidades.index(u_valor) if u_valor in lista_unidades else 0
                
                e_um = st.selectbox("Medida de Conteo", lista_unidades, index=u_idx)
                e_min = st.number_input("Stock Mínimo", value=float(datos_act.get("Stock Mínimo", 0) or 0))
                
                if st.form_submit_button("💾 Guardar Cambios"):
                    idx_fila = df_raw[df_raw["Nombre del Insumo"] == insumo_a_editar].index[0] + 2
                    actualizacion = [e_u, e_n, e_m, e_p, e_g, "", e_uc, e_um, "", "", "", e_min]
                    insumos_sheet.update(f'A{idx_fila}:L{idx_fila}', [actualizacion])
                    st.cache_data.clear(); st.success("Actualizado"); st.rerun()

# --- PÁGINA: PORTADA ---
if st.session_state.pagina == "Portada":
    st.title("📊 Resumen Operativo")
    if not df_historial.empty:
        df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
        ultimo_estado = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
        por_comprar = ultimo_estado[ultimo_estado['Necesita Compra'].astype(str).str.upper() == "TRUE"]
        
        c1, c2 = st.columns(2)
        with c1: st.metric("Noble: Críticos", len(por_comprar[por_comprar['Unidad de Negocio'] == "Noble"]))
        with c2: st.metric("Coffee Station: Críticos", len(por_comprar[por_comprar['Unidad de Negocio'] == "Coffee Station"]))
        
        sucursal = st.radio("Sucursal:", ["Noble", "Coffee Station"], horizontal=True)
        lista = por_comprar[por_comprar['Unidad de Negocio'] == sucursal]
        if not lista.empty:
            st.dataframe(lista[['Nombre del Insumo', 'Grupo', 'Stock Neto', 'Stock Mínimo', 'Responsable']], use_container_width=True, hide_index=True)
        else: st.success("✅ Stock Completo")
    else: st.info("Realiza tu primer inventario.")
    if st.button("🚀 INICIAR INVENTARIO", use_container_width=True, type="primary"): cambiar_pagina("Inventario")

# --- PÁGINA: INVENTARIO ---
elif st.session_state.pagina == "Inventario":
    st.title("📝 Registro de Inventario")
    col_u, col_r, col_g = st.columns(3)
    with col_u: u_sel = st.selectbox("🏢 Unidad", sorted(df_raw["Unidad de Negocio"].unique().tolist()) if not df_raw.empty else ["Noble"])
    with col_r: resp = st.selectbox("👤 Responsable", st.session_state.responsables)
    with col_g:
        df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel]
        g_sel = st.selectbox("📂 Grupo", sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"])

    df_f = df_u[df_u["Grupo"] == g_sel].reset_index(drop=True)
    if not df_f.empty:
        registros = {}
        for idx, row in df_f.iterrows():
            c1, c2, c3, c4 = st.columns([3, 1.5, 1.5, 1.5])
            with c1: 
                st.write(f"**{row['Nombre del Insumo']}**")
                st.caption(f"{row.get('Marca','')} | {row.get('Presentación de Compra','')}")
            with c2: alm = st.number_input("Almacén", min_value=0.0, step=1.0, key=f"a{idx}")
            with c3: act = st.number_input("Barra", min_value=0.0, step=0.1, key=f"t{idx}")
            with c4:
                neto = alm + act
                st.metric("Neto", f"{neto:.1f} {row.get('Unidad de Medida','pz')}")
            registros[row['Nombre del Insumo']] = {"alm": alm, "act": act, "neto": neto, "row": row}
            st.divider()
        if st.button("📥 GUARDAR INVENTARIO", use_container_width=True, type="primary"):
            filas = []
            f_hoy = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for nom, info in registros.items():
                dm = info["row"]
                s_min = float(dm.get("Stock Mínimo", 0) or 0)
                compra = "TRUE" if info["neto"] <= s_min else "FALSE"
                filas.append([u_sel, nom, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                              dm.get('Presentación de Compra',''), dm.get('Unidad de Medida','pz'), 
                              info["alm"], info["act"], info["neto"], s_min, compra, resp, f_hoy])
            historial_sheet.append_rows(filas)
            st.cache_data.clear(); st.success("✅ Guardado"); st.balloons()
