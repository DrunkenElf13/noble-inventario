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
    st.error(f"Error de conexión: {e}")
    st.stop()

# 2. CARGA DE DATOS (CON MANEJO DE CACHÉ Y ERRORES)
@st.cache_data(ttl=10)
def cargar_datos_maestros():
    try:
        data = insumos_sheet.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except: return pd.DataFrame()

@st.cache_data(ttl=10)
def cargar_historial():
    try:
        data = historial_sheet.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except: return pd.DataFrame()

df_raw = cargar_datos_maestros()
df_historial = cargar_historial()

# 3. NAVEGACIÓN
if "pagina" not in st.session_state: st.session_state.pagina = "Dashboard"
def cambiar_pagina(nombre):
    st.session_state.pagina = nombre
    st.rerun()

# --- SIDEBAR: GESTIÓN INTEGRAL ---
with st.sidebar:
    st.title("⚙️ Operaciones")
    if st.button("📊 Dashboard Principal", use_container_width=True): cambiar_pagina("Dashboard")
    if st.button("📝 Registrar Inventario", use_container_width=True): cambiar_pagina("Inventario")
    
    st.divider()
    st.write("**Tickets (58mm):**")
    if st.button("🖨️ 1. Lista de Conteo", use_container_width=True): cambiar_pagina("Impresion")
    if st.button("🛒 2. Lista de Compra", use_container_width=True): cambiar_pagina("ListaCompra")
    
    st.divider()
    if "responsables" not in st.session_state:
        st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]
    
    with st.expander("👤 Equipo"):
        n_nom = st.text_input("Nuevo Barista:")
        if st.button("➕ Agregar"):
            if n_nom: st.session_state.responsables.append(n_nom); st.rerun()

    st.divider()
    st.subheader("📦 Catálogo de Insumos")
    op_insumo = st.radio("Acción:", ["Añadir Insumo", "Editar Insumo"])

    if op_insumo == "Añadir Insumo":
        with st.form("f_add", clear_on_submit=True):
            u = st.selectbox("Unidad", ["Noble", "Coffee Station"])
            n = st.text_input("Nombre")
            m = st.text_input("Marca")
            p = st.text_input("Proveedor")
            g = st.selectbox("Grupo", ["A", "B", "C", "D", "E", "F"])
            uc = st.text_input("Pres. Compra")
            um = st.selectbox("Medida", ["pz", "ml", "gr", "%", "kg", "lt"])
            sm = st.number_input("Stock Mínimo", min_value=0.0)
            if st.form_submit_button("✨ Crear"):
                insumos_sheet.append_row([u, n, m, p, g, "", uc, um, "", "", "", sm])
                st.cache_data.clear(); st.rerun()
    else:
        if not df_raw.empty and "Nombre del Insumo" in df_raw.columns:
            ins_editar = st.selectbox("Editar:", df_raw["Nombre del Insumo"].tolist())
            d = df_raw[df_raw["Nombre del Insumo"] == ins_editar].iloc[0]
            with st.form("f_edit"):
                e_u = st.selectbox("Unidad", ["Noble", "Coffee Station"], index=0 if d["Unidad de Negocio"]=="Noble" else 1)
                e_n = st.text_input("Nombre", value=str(d["Nombre del Insumo"]))
                e_m = st.text_input("Marca", value=str(d.get("Marca","")))
                e_p = st.text_input("Proveedor", value=str(d.get("Proveedor","")))
                e_g = st.selectbox("Grupo", ["A","B","C","D","E","F"], index=["A","B","C","D","E","F"].index(d.get("Grupo","A")))
                e_uc = st.text_input("Unidad Compra", value=str(d.get("Presentación de Compra","")))
                list_u = ["pz", "ml", "gr", "%", "kg", "lt"]; u_val = str(d.get("Unidad de Medida","pz")).lower()
                e_um = st.selectbox("Medida", list_u, index=list_u.index(u_val) if u_val in list_u else 0)
                try: v_init_m = float(d.get("Stock Mínimo", 0) or 0)
                except: v_init_m = 0.0
                e_sm = st.number_input("Mínimo", value=v_init_m)
                if st.form_submit_button("💾 Actualizar"):
                    idx = df_raw[df_raw["Nombre del Insumo"] == ins_editar].index[0] + 2
                    insumos_sheet.update(range_name=f'A{idx}:L{idx}', values=[[e_u, e_n, e_m, e_p, e_g, "", e_uc, e_um, "", "", "", e_sm]])
                    st.cache_data.clear(); st.rerun()

# --- PÁGINA: DASHBOARD ---
if st.session_state.pagina == "Dashboard":
    st.title("📊 Dashboard Operativo")
    if not df_historial.empty:
        df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
        ultimo = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
        criticos = ultimo[ultimo['Necesita Compra'].astype(str).str.upper() == "TRUE"]
        
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Pendientes Noble", len(criticos[criticos['Unidad de Negocio']=="Noble"]))
        with c2: st.metric("Pendientes Coffee Station", len(criticos[criticos['Unidad de Negocio']=="Coffee Station"]))
        with c3: st.metric("Última Carga", df_historial['Fecha de Inventario'].max().strftime("%d/%m %H:%M"))
        
        st.divider()
        col_n, col_cs = st.columns(2)
        with col_n:
            st.subheader("🏢 Noble")
            ins_n = criticos[criticos['Unidad de Negocio'] == "Noble"]
            for _, r in ins_n.iterrows(): st.warning(f"**{r['Nombre del Insumo']}** (Hay: {r['Stock Neto']})")
        with col_cs:
            st.subheader("☕ Coffee Station")
            ins_cs = criticos[criticos['Unidad de Negocio'] == "Coffee Station"]
            for _, r in ins_cs.iterrows(): st.warning(f"**{r['Nombre del Insumo']}** (Hay: {r['Stock Neto']})")
        
        st.divider(); st.subheader("🕒 Actividad Reciente")
        st.dataframe(df_historial.sort_values('Fecha de Inventario', ascending=False).head(10), use_container_width=True)
    else: st.info("Sin datos históricos. Inicia un inventario.")

# --- PÁGINA: INVENTARIO (CAPTURA DIGITAL COMPLETA) ---
elif st.session_state.pagina == "Inventario":
    st.title("📝 Levantamiento de Stock")
    c_u, c_r, c_g = st.columns([1, 1, 2])
    with c_u: u_sel = st.selectbox("🏢 Unidad", ["Noble", "Coffee Station"])
    with c_r: r_sel = st.selectbox("👤 Responsable", st.session_state.responsables)
    with c_g:
        df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
        grps = sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"]
        g_sel = st.multiselect("📂 Grupos", grps, default=grps[:1])

    ultimo_reg = pd.DataFrame()
    if not df_historial.empty:
        try:
            df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
            ultimo_reg = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
        except: pass

    df_f = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"]).reset_index(drop=True)
    if not df_f.empty:
        regs = {}
        h1, h2, h3, h4, h5, h6 = st.columns([2.5, 1, 1, 1, 1, 1.2])
        with h1: st.write("**Insumo / Ref**"); with h2: st.write("**Alm.**")
        with h3: st.write("**Barra**"); with h4: st.write("**Medida**")
        with h5: st.write("**Neto**"); with h6: st.write("**¿Pedir? 🛒**")
        st.divider()

        for i, row in df_f.iterrows():
            nom = row['Nombre del Insumo']
            v_prev = 0.0
            if not ultimo_reg.empty:
                m = ultimo_reg[(ultimo_reg['Unidad de Negocio']==u_sel) & (ultimo_reg['Nombre del Insumo']==nom)]
                if not m.empty: v_prev = float(m.iloc[0].get('Stock Neto', 0.0))
            try: v_min = float(row.get('Stock Mínimo', 0) or 0)
            except: v_min = 0.0

            with st.container():
                c1, c2, c3, c4, c5, c6 = st.columns([2.5, 1, 1, 1, 1, 1.2])
                with c1: 
                    st.write(f"**{nom}**")
                    diff = v_prev - v_min; color = "green" if diff > 0 else "red"
                    st.markdown(f"<small>Ant: {v_prev} | Mín: {v_min} (<span style='color:{color}'>{diff:+.1f}</span>)</small>", unsafe_allow_html=True)
                with c2: v_a = st.number_input("A", min_value=0.0, key=f"a{i}", label_visibility="collapsed")
                with c3: v_b = st.number_input("B", min_value=0.0, key=f"b{i}", label_visibility="collapsed")
                with c4:
                    u_list = ["pz", "ml", "gr", "%", "kg", "lt"]; u_act = str(row.get("Unidad de Medida","pz")).lower()
                    v_u = st.selectbox("M", u_list, index=u_list.index(u_act) if u_act in u_list else 0, key=f"u{i}", label_visibility="collapsed")
                with c5: v_n = v_a + v_b; st.write(f"**{v_n:.1f}**")
                with c6: v_p = st.toggle("Pedir", value=False, key=f"p{i}", label_visibility="collapsed")
                regs[nom] = {"alm": v_a, "bar": v_b, "neto": v_n, "um": v_u, "ped": v_p, "row": row}
            st.divider()

        if st.button("📥 GUARDAR INVENTARIO", use_container_width=True, type="primary"):
            filas = []
            fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for n, info in regs.items():
                dm = info["row"]
                filas.append([u_sel, n, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                              dm.get('Presentación de Compra',''), info["um"], info["alm"], info["bar"], 
                              info["neto"], dm.get('Stock Mínimo',0), "TRUE" if info["ped"] else "FALSE", r_sel, fh])
            historial_sheet.append_rows(filas); st.cache_data.clear(); st.success("Guardado"); st.balloons()

# --- PÁGINAS DE TICKETS (58MM) ---
elif st.session_state.pagina == "Impresion":
    st.title("🖨️ Ticket de Conteo")
    u_sel = st.selectbox("Sucursal", ["Noble", "Coffee Station"])
    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    g_sel = st.multiselect("Grupos", sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"])
    df_p = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"])
    if not df_p.empty:
        t = f"*** CONTEO {u_sel.upper()} ***\n" + "-"*22 + "\n"
        for _, r in df_p.iterrows(): t += f"[ ] {r['Nombre del Insumo'][:20]}\n"
        st.text_area("Ticket:", value=t + "-"*22, height=400)

elif st.session_state.pagina == "ListaCompra":
    st.title("🛒 Ticket de Compra")
    u_sel = st.radio("Sucursal:", ["Noble", "Coffee Station"], horizontal=True)
    if not df_historial.empty:
        ult = df_historial[df_historial['Unidad de Negocio']==u_sel].sort_values('Fecha de Inventario').drop_duplicates('Nombre del Insumo', keep='last')
        com = ult[ult['Necesita Compra'].astype(str).str.upper()=="TRUE"]
        if not com.empty:
            t = f"*** COMPRAS {u_sel.upper()} ***\n" + "-"*22 + "\n"
            for _, r in com.iterrows(): t += f"• {r['Nombre del Insumo'][:18]}\n  Hay: {r['Stock Neto']} | Min: {r['Stock Mínimo']}\n\n"
            st.text_area("Ticket:", value=t + "-"*22, height=400)
        else: st.success("Nada marcado para compra.")
