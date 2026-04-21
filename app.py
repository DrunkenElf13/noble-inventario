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

# 2. CARGA DE DATOS (CON PREVENCIÓN DE ERRORES)
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

# 3. ESTADO DE NAVEGACIÓN
if "pagina" not in st.session_state: st.session_state.pagina = "Portada"
def cambiar_pagina(nombre):
    st.session_state.pagina = nombre
    st.rerun()

# --- SIDEBAR: GESTIÓN DE EQUIPO Y CATÁLOGO ---
with st.sidebar:
    st.title("⚙️ Operaciones")
    if st.button("🏠 Portada / Críticos", use_container_width=True): cambiar_pagina("Portada")
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
            if n_nom: 
                st.session_state.responsables.append(n_nom)
                st.rerun()

    st.divider()
    st.subheader("📦 Catálogo")
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
            sm = st.number_input("Mínimo", min_value=0.0)
            if st.form_submit_button("✨ Crear"):
                insumos_sheet.append_row([u, n, m, p, g, "", uc, um, "", "", "", sm])
                st.cache_data.clear()
                st.rerun()
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
                list_u = ["pz", "ml", "gr", "%", "kg", "lt"]
                u_val = str(d.get("Unidad de Medida","pz")).lower()
                e_um = st.selectbox("Medida", list_u, index=list_u.index(u_val) if u_val in list_u else 0)
                try: v_init_m = float(d.get("Stock Mínimo", 0) or 0)
                except: v_init_m = 0.0
                e_sm = st.number_input("Mínimo", value=v_init_m)
                if st.form_submit_button("💾 Actualizar"):
                    idx = df_raw[df_raw["Nombre del Insumo"] == ins_editar].index[0] + 2
                    insumos_sheet.update(range_name=f'A{idx}:L{idx}', values=[[e_u, e_n, e_m, e_p, e_g, "", e_uc, e_um, "", "", "", e_sm]])
                    st.cache_data.clear()
                    st.rerun()

# --- PÁGINA: PORTADA ---
if st.session_state.pagina == "Portada":
    st.title("📊 Resumen de Críticos")
    if not df_historial.empty and "Necesita Compra" in df_historial.columns:
        try:
            df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
            ultimo = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
            criticos = ultimo[ultimo['Necesita Compra'].astype(str).str.upper() == "TRUE"]
            
            c1, c2 = st.columns(2)
            with c1: st.metric("Noble", len(criticos[criticos['Unidad de Negocio']=="Noble"]))
            with c2: st.metric("Coffee Station", len(criticos[criticos['Unidad de Negocio']=="Coffee Station"]))
            
            st.divider()
            suc = st.radio("Sucursal:", ["Noble", "Coffee Station"], horizontal=True)
            lista = criticos[criticos['Unidad de Negocio'] == suc]
            if not lista.empty:
                for _, r in lista.iterrows():
                    with st.expander(f"🔴 {r['Nombre del Insumo']} (Stock: {r['Stock Neto']})"):
                        st.write(f"Marca: {r.get('Marca','')} | Proveedor: {r.get('Proveedor','')}")
            else: st.success(f"✅ Todo al día en {suc}")
        except: st.info("Sincronizando historial...")
    else: st.info("Realiza un inventario para ver los insumos críticos.")

# --- PÁGINA: INVENTARIO (CAPTURA DIGITAL) ---
elif st.session_state.pagina == "Inventario":
    st.title("📝 Levantamiento de Stock")
    
    if df_raw.empty:
        st.warning("El catálogo está vacío. Agrega insumos en el menú lateral.")
    else:
        c_u, c_r, c_g = st.columns([1, 1, 2])
        with c_u: u_sel = st.selectbox("🏢 Unidad", ["Noble", "Coffee Station"])
        with c_r: r_sel = st.selectbox("👤 Responsable", st.session_state.responsables)
        with c_g:
            df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel]
            grps_disp = sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"]
            g_sel = st.multiselect("📂 Grupos a contar", grps_disp, default=grps_disp[:1])

        # Obtener stock previo seguro
        ultimo_reg = pd.DataFrame()
        if not df_historial.empty and "Nombre del Insumo" in df_historial.columns:
            try:
                df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
                ultimo_reg = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
            except: pass

        df_f = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"]).reset_index(drop=True)
        
        if not df_f.empty:
            regs = {}
            # Encabezados (Corregidos sin sintaxis inválida)
            h1, h2, h3, h4, h5, h6 = st.columns([2.5, 1, 1, 1, 1, 1.2])
            with h1: st.write("**Insumo / Referencia**")
            with h2: st.write("**Alm.**")
            with h3: st.write("**Barra**")
            with h4: st.write("**Medida**")
            with h5: st.write("**Neto**")
            with h6: st.write("**¿Pedir? 🛒**")
            st.divider()

            for i, row in df_f.iterrows():
                nom = row['Nombre del Insumo']
                v_prev = 0.0
                if not ultimo_reg.empty:
                    try:
                        m = ultimo_reg[(ultimo_reg['Unidad de Negocio']==u_sel) & (ultimo_reg['Nombre del Insumo']==nom)]
                        if not m.empty: v_prev = float(m.iloc[0].get('Stock Neto', 0.0))
                    except: v_prev = 0.0
                
                try: v_min = float(row.get('Stock Mínimo', 0) or 0)
                except: v_min = 0.0
                
                with st.container():
                    c1, c2, c3, c4, c5, c6 = st.columns([2.5, 1, 1, 1, 1, 1.2])
                    with c1: 
                        st.write(f"**{nom}**")
                        st.caption(f"Marca: {row.get('Marca','-')} | Prov: {row.get('Proveedor','-')}")
                        diff = v_prev - v_min
                        color = "green" if diff > 0 else "red"
                        st.markdown(f"<small>Ant: {v_prev} | Mín: {v_min} (<span style='color:{color}'>{diff:+.1f}</span>)</small>", unsafe_allow_html=True)
                    with c2: v_a = st.number_input("A", min_value=0.0, step=1.0, key=f"a{i}", label_visibility="collapsed")
                    with c3: v_b = st.number_input("B", min_value=0.0, step=0.1, key=f"b{i}", label_visibility="collapsed")
                    with c4:
                        u_list = ["pz", "ml", "gr", "%", "kg", "lt"]
                        u_act = str(row.get("Unidad de Medida","pz")).lower()
                        v_u = st.selectbox("M", u_list, index=u_list.index(u_act) if u_act in u_list else 0, key=f"u{i}", label_visibility="collapsed")
                    with c5: 
                        v_n = v_a + v_b
                        st.write(f"**{v_n:.1f}**")
                    with c6: v_p = st.toggle("Pedir", value=False, key=f"p{i}", label_visibility="collapsed")
                    regs[nom] = {"alm": v_a, "bar": v_b, "neto": v_n, "um": v_u, "ped": v_p, "row": row}
                st.divider()

            if st.button("📥 GUARDAR EN LA NUBE", use_container_width=True, type="primary"):
                filas = []
                fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for n, info in regs.items():
                    dm = info["row"]
                    filas.append([u_sel, n, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                                  dm.get('Presentación de Compra',''), info["um"], info["alm"], info["bar"], 
                                  info["neto"], dm.get('Stock Mínimo',0), "TRUE" if info["ped"] else "FALSE", r_sel, fh])
                historial_sheet.append_rows(filas)
                st.cache_data.clear()
                st.success("¡Inventario guardado con éxito!")
                st.balloons()

# --- PÁGINA: TICKET CONTEO ---
elif st.session_state.pagina == "Impresion":
    st.title("🖨️ Ticket de Conteo (58mm)")
    u_sel = st.selectbox("🏢 Sucursal", ["Noble", "Coffee Station"])
    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    g_sel = st.multiselect("📂 Filtrar por Grupos", sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"])
    df_p = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"])
    if not df_p.empty:
        t = f"*** CONTEO {u_sel.upper()} ***\n" + "-"*22 + "\n"
        for _, r in df_p.iterrows(): t += f"[ ] {r['Nombre del Insumo'][:20]}\n"
        st.text_area("Copia este texto para la impresora:", value=t + "-"*22, height=400)

# --- PÁGINA: TICKET COMPRA ---
elif st.session_state.pagina == "ListaCompra":
    st.title("🛒 Ticket de Compra (58mm)")
    u_sel = st.radio("Sucursal:", ["Noble", "Coffee Station"], horizontal=True)
    if not df_historial.empty:
        try:
            df_historial['Fecha de Inventario'] = pd.to_datetime(df_historial['Fecha de Inventario'])
            ult = df_historial[df_historial['Unidad de Negocio']==u_sel].sort_values('Fecha de Inventario').drop_duplicates('Nombre del Insumo', keep='last')
            com = ult[ult['Necesita Compra'].astype(str).str.upper()=="TRUE"]
            if not com.empty:
                t = f"*** COMPRAS {u_sel.upper()} ***\n" + "-"*22 + "\n"
                for _, r in com.iterrows():
                    t += f"• {r['Nombre del Insumo'][:18]}\n  Hay: {r['Stock Neto']} | Min: {r['Stock Mínimo']}\n\n"
                st.text_area("Copia este texto para la impresora:", value=t + "-"*22, height=400)
            else: st.success("No hay nada marcado para compra hoy.")
        except: st.error("Error al procesar el historial.")
