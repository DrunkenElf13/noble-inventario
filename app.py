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
    data = historial_sheet.get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame()

df_raw = cargar_datos_maestros()
df_historial = cargar_historial()

# 3. NAVEGACIÓN
if "pagina" not in st.session_state:
    st.session_state.pagina = "Portada"

def cambiar_pagina(nombre):
    st.session_state.pagina = nombre
    st.rerun()

# --- SIDEBAR ---
with st.sidebar:
    st.title("⚙️ Operaciones")
    if st.button("🏠 Portada / Compras", use_container_width=True): cambiar_pagina("Portada")
    if st.button("📝 Registrar Inventario", use_container_width=True): cambiar_pagina("Inventario")
    
    st.divider()
    if "responsables" not in st.session_state:
        st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]
    
    with st.expander("👤 Equipo"):
        n_nom = st.text_input("Nuevo Barista:")
        if st.button("➕ Agregar"):
            if n_nom: st.session_state.responsables.append(n_nom); st.rerun()

    st.divider()
    op_insumo = st.radio("Insumos:", ["Añadir", "Editar"])

    if op_insumo == "Añadir":
        with st.form("f_add"):
            u = st.selectbox("Unidad", ["Noble", "Coffee Station"])
            n = st.text_input("Nombre")
            m = st.text_input("Marca")
            p = st.text_input("Proveedor")
            g = st.selectbox("Grupo", ["A", "B", "C", "D", "E", "F"])
            uc = st.text_input("Presentación de Compra")
            um = st.selectbox("Unidad de Medida", ["pz", "ml", "gr", "%", "kg", "lt"])
            sm = st.number_input("Mínimo", min_value=0.0)
            if st.form_submit_button("✨ Crear"):
                insumos_sheet.append_row([u, n, m, p, g, "", uc, um, "", "", "", sm])
                st.cache_data.clear(); st.rerun()
    else:
        ins_editar = st.selectbox("Insumo a editar:", df_raw["Nombre del Insumo"].tolist() if not df_raw.empty else [])
        if ins_editar:
            d = df_raw[df_raw["Nombre del Insumo"] == ins_editar].iloc[0]
            with st.form("f_edit"):
                e_u = st.selectbox("Unidad", ["Noble", "Coffee Station"], index=0 if d["Unidad de Negocio"] == "Noble" else 1)
                e_n = st.text_input("Nombre", value=str(d["Nombre del Insumo"]))
                e_m = st.text_input("Marca", value=str(d.get("Marca","")))
                e_p = st.text_input("Proveedor", value=str(d.get("Proveedor","")))
                e_g = st.selectbox("Grupo", ["A","B","C","D","E","F"], index=["A","B","C","D","E","F"].index(d.get("Grupo","A")))
                e_uc = st.text_input("Unidad de Compra", value=str(d.get("Presentación de Compra","")))
                list_u = ["pz", "ml", "gr", "%", "kg", "lt"]
                u_val = str(d.get("Unidad de Medida","pz")).lower()
                e_um = st.selectbox("Medida", list_u, index=list_u.index(u_val) if u_val in list_u else 0)
                e_sm = st.number_input("Mínimo", value=float(d.get("Stock Mínimo",0) or 0))
                if st.form_submit_button("💾 Actualizar"):
                    idx = df_raw[df_raw["Nombre del Insumo"] == ins_editar].index[0] + 2
                    insumos_sheet.update(f'A{idx}:L{idx}', [[e_u, e_n, e_m, e_p, e_g, "", e_uc, e_um, "", "", "", e_sm]])
                    st.cache_data.clear(); st.rerun()

# --- PÁGINA: PORTADA ---
if st.session_state.pagina == "Portada":
    st.title("📊 Resumen de Compras")
    if not df_historial.empty and "Necesita Compra" in df_historial.columns:
        df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
        ultimo = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
        criticos = ultimo[ultimo['Necesita Compra'].astype(str).str.upper() == "TRUE"]
        
        c1, c2 = st.columns(2)
        with c1: st.metric("Noble: Críticos", len(criticos[criticos['Unidad de Negocio']=="Noble"]))
        with c2: st.metric("Coffee Station: Críticos", len(criticos[criticos['Unidad de Negocio']=="Coffee Station"]))
        
        suc = st.radio("Ver Sucursal:", ["Noble", "Coffee Station"], horizontal=True)
        lista = criticos[criticos['Unidad de Negocio'] == suc]
        if not lista.empty:
            for _, r in lista.iterrows():
                with st.expander(f"🔴 {r['Nombre del Insumo']} (Neto: {r['Stock Neto']})"):
                    st.write(f"**Marca:** {r.get('Marca','')} | **Proveedor:** {r.get('Proveedor','')}")
                    st.caption(f"Registrado por {r['Responsable']} el {r['Fecha de Inventario'].strftime('%d/%m %H:%M')}")
        else: st.success(f"✅ Sin faltantes en {suc}")
    else: st.info("No hay datos en el historial. Inicia un levantamiento.")
    if st.button("🚀 INICIAR INVENTARIO", use_container_width=True, type="primary"): cambiar_pagina("Inventario")

# --- PÁGINA: INVENTARIO ---
elif st.session_state.pagina == "Inventario":
    st.title("📝 Registro de Stock")
    c_u, c_r, c_g = st.columns(3)
    with c_u: u_sel = st.selectbox("🏢 Sucursal", ["Noble", "Coffee Station"])
    with c_r: r_sel = st.selectbox("👤 Responsable", st.session_state.responsables)
    with c_g:
        df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel]
        grps = sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"]
        g_sel = st.selectbox("📂 Grupo", grps)

    # Lógica segura para obtener el último registro
    ultimo_registro = pd.DataFrame()
    if not df_historial.empty and "Unidad de Negocio" in df_historial.columns:
        df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
        ultimo_registro = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')

    df_f = df_u[df_u["Grupo"] == g_sel].reset_index(drop=True)
    if not df_f.empty:
        regs = {}
        h1, h2, h3, h4, h5, h6 = st.columns([2.5, 1, 1, 1, 1, 1])
        with h1: st.write("**Insumo / Referencia**")
        with h2: st.write("**Alm.**")
        with h3: st.write("**Barra**")
        with h4: st.write("**Medida**")
        with h5: st.write("**Neto**")
        with h6: st.write("**¿Pedir?**")
        st.divider()

        for i, row in df_f.iterrows():
            nom_ins = row['Nombre del Insumo']
            
            # Obtención segura de datos previos para evitar KeyError
            v_prev = 0.0
            if not ultimo_registro.empty:
                prev_match = ultimo_registro[(ultimo_registro['Unidad de Negocio'] == u_sel) & (ultimo_registro['Nombre del Insumo'] == nom_ins)]
                if not prev_match.empty:
                    v_prev = prev_match.iloc[0].get('Stock Neto', 0.0)

            v_min = float(row.get('Stock Mínimo', 0) or 0)
            
            c1, c2, c3, c4, c5, c6 = st.columns([2.5, 1, 1, 1, 1, 1])
            with c1: 
                st.write(f"**{nom_ins}**")
                st.caption(f"Marca: {row.get('Marca','n/a')} | Prov: {row.get('Proveedor','n/a')}")
                diff = v_prev - v_min
                color_diff = "green" if diff > 0 else "red"
                st.markdown(f"<small>Anterior: <b>{v_prev}</b> | Mín: <b>{v_min}</b> (<span style='color:{color_diff}'>{diff:+.1f}</span>)</small>", unsafe_allow_html=True)

            with c2: v_alm = st.number_input("A", min_value=0.0, step=1.0, key=f"a{i}", label_visibility="collapsed")
            with c3: v_bar = st.number_input("B", min_value=0.0, step=0.1, key=f"b{i}", label_visibility="collapsed")
            with c4:
                unid_list = ["pz", "ml", "gr", "%", "kg", "lt"]
                u_curr = str(row.get("Unidad de Medida","pz")).lower()
                v_uni = st.selectbox("M", unid_list, index=unid_list.index(u_curr) if u_curr in unid_list else 0, key=f"u{i}", label_visibility="collapsed")
            with c5:
                v_neto = v_alm + v_bar
                st.write(f"**{v_neto:.1f}**")
            with c6: v_com = st.toggle("P", value=False, key=f"p{i}")
            
            regs[nom_ins] = {"alm": v_alm, "bar": v_bar, "neto": v_neto, "um": v_uni, "ped": v_com, "row": row}
            st.divider()

        if st.button("📥 GUARDAR REGISTRO", use_container_width=True, type="primary"):
            filas = []
            fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for n, info in regs.items():
                dm = info["row"]
                pedir = "TRUE" if info["ped"] else "FALSE"
                filas.append([u_sel, n, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                              dm.get('Presentación de Compra',''), info["um"], info["alm"], info["bar"], 
                              info["neto"], dm.get('Stock Mínimo',0), pedir, r_sel, fh])
            historial_sheet.append_rows(filas)
            st.cache_data.clear(); st.success("✅ Datos guardados."); st.balloons()
