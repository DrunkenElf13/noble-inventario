import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Noble & Coffee Station", page_icon="☕", layout="wide")

# --- 1. CONEXIÓN A GOOGLE SHEETS ---
@st.cache_resource
def conectar_google_sheets():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
        client = gspread.authorize(creds)
        return client.open_by_key("1VZV81p-JqoaRPzMzsRurF6wntVefyaN5ozs3RJe6uJs")
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

sh = conectar_google_sheets()

# --- 2. FUNCIONES DE LIMPIEZA Y CARGA (BLINDAJE CONTRA % Y NULOS) ---
def limpiar_valor(valor):
    if valor is None or valor == "": return 0.0
    if isinstance(valor, (int, float)): return float(valor)
    s = str(valor).replace('%', '').strip()
    try: return float(s)
    except: return 0.0

@st.cache_data(ttl=10)
def cargar_datos_integrales():
    if not sh: return pd.DataFrame(), pd.DataFrame()
    try:
        ins = pd.DataFrame(sh.worksheet("Insumos").get_all_records())
        his = pd.DataFrame(sh.worksheet("Historial").get_all_records())
        if not his.empty and 'Fecha de Inventario' in his.columns:
            his['Fecha de Inventario'] = pd.to_datetime(his['Fecha de Inventario'])
        return ins, his
    except: return pd.DataFrame(), pd.DataFrame()

df_raw, df_historial = cargar_datos_integrales()

# --- 3. NAVEGACIÓN ---
if "pagina" not in st.session_state: st.session_state.pagina = "Dashboard"
def cambiar_pagina(nombre):
    st.session_state.pagina = nombre
    st.rerun()

# --- 4. SIDEBAR (GESTIÓN DE EQUIPO + CATÁLOGO + NAVEGACIÓN) ---
with st.sidebar:
    st.title("☕ Noble & Coffee Station")
    if st.button("📊 Dashboard Principal", use_container_width=True): cambiar_pagina("Dashboard")
    if st.button("📝 Registrar Inventario", use_container_width=True): cambiar_pagina("Inventario")
    
    st.divider()
    st.write("**Tickets (58mm):**")
    if st.button("🖨️ 1. Lista de Conteo", use_container_width=True): cambiar_pagina("Impresion")
    if st.button("🛒 2. Lista de Compra", use_container_width=True): cambiar_pagina("ListaCompra")
    
    st.divider()
    if "responsables" not in st.session_state:
        st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]
    
    with st.expander("👤 Equipo Noble"):
        n_barista = st.text_input("Nuevo Barista:")
        if st.button("➕ Agregar"):
            if n_barista: 
                st.session_state.responsables.append(n_barista)
                st.rerun()

    st.divider()
    st.subheader("📦 Gestión del Catálogo")
    op_cat = st.radio("Acción:", ["Añadir Insumo", "Editar Insumo"])

    if op_cat == "Añadir Insumo":
        with st.form("f_add", clear_on_submit=True):
            u = st.selectbox("Unidad", ["Noble", "Coffee Station"])
            n = st.text_input("Nombre")
            m = st.text_input("Marca")
            p = st.text_input("Proveedor")
            g = st.selectbox("Grupo", ["A", "B", "C", "D", "E", "F"])
            um = st.selectbox("Medida", ["pz", "ml", "gr", "%", "kg", "lt"])
            sm = st.number_input("Stock Mínimo", min_value=0.0)
            if st.form_submit_button("✨ Crear Insumo"):
                sh.worksheet("Insumos").append_row([u, n, m, p, g, "", "", um, "", "", "", sm])
                st.cache_data.clear(); st.rerun()
    else:
        if not df_raw.empty:
            ins_edit = st.selectbox("Insumo a Editar:", df_raw["Nombre del Insumo"].tolist())
            d = df_raw[df_raw["Nombre del Insumo"] == ins_edit].iloc[0]
            with st.form("f_edit"):
                e_n = st.text_input("Nombre", value=str(d["Nombre del Insumo"]))
                e_g = st.selectbox("Grupo", ["A","B","C","D","E","F"], index=["A","B","C","D","E","F"].index(d.get("Grupo","A")))
                e_sm = st.number_input("Stock Mínimo", value=limpiar_valor(d.get("Stock Mínimo", 0)))
                if st.form_submit_button("💾 Actualizar"):
                    idx = df_raw[df_raw["Nombre del Insumo"] == ins_edit].index[0] + 2
                    sh.worksheet("Insumos").update_cell(idx, 2, e_n)
                    sh.worksheet("Insumos").update_cell(idx, 5, e_g)
                    sh.worksheet("Insumos").update_cell(idx, 12, e_sm)
                    st.cache_data.clear(); st.rerun()

# --- 5. PÁGINA: DASHBOARD ---
if st.session_state.pagina == "Dashboard":
    st.title("📊 Dashboard Operativo")
    if not df_historial.empty:
        try:
            # Obtener el último estado de cada insumo
            ult = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
            crit = ult[ult['Necesita Compra'].astype(str).str.upper() == "TRUE"]
            
            c1, c2, c3 = st.columns(3)
            with c1: st.metric("Pendientes Noble", len(crit[crit['Unidad de Negocio']=="Noble"]))
            with c2: st.metric("Pendientes Coffee Station", len(crit[crit['Unidad de Negocio']=="Coffee Station"]))
            with c3: st.metric("Último Inventario", df_historial['Fecha de Inventario'].max().strftime("%d/%m %H:%M"))
            
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("🏢 Faltantes: Noble")
                for _, r in crit[crit['Unidad de Negocio']=="Noble"].iterrows():
                    st.error(f"**{r['Nombre del Insumo']}** (Stock: {r['Stock Neto']} / Mín: {r['Stock Mínimo']})")
            with col2:
                st.subheader("☕ Faltantes: Coffee Station")
                for _, r in crit[crit['Unidad de Negocio']=="Coffee Station"].iterrows():
                    st.error(f"**{r['Nombre del Insumo']}** (Stock: {r['Stock Neto']} / Mín: {r['Stock Mínimo']})")
            
            st.divider()
            st.subheader("🕒 Actividad Reciente (Logs)")
            st.dataframe(df_historial.sort_values('Fecha de Inventario', ascending=False).head(15), use_container_width=True)
        except Exception as e: st.error(f"Error en Dashboard: {e}")
    else: st.info("Sin registros históricos aún.")

# --- 6. PÁGINA: INVENTARIO (CAPTURA DIGITAL) ---
elif st.session_state.pagina == "Inventario":
    st.title("📝 Levantamiento de Stock")
    
    col_u, col_r, col_g = st.columns([1, 1, 2])
    with col_u: u_sel = st.selectbox("🏢 Unidad", ["Noble", "Coffee Station"])
    with col_r: r_sel = st.selectbox("👤 Responsable", st.session_state.responsables)
    with col_g:
        df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
        grps = sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"]
        g_sel = st.multiselect("📂 Grupos a contar", grps, default=grps[:1])

    # Cargar stock anterior para referencia visual
    ultimo_reg = pd.DataFrame()
    if not df_historial.empty:
        ultimo_reg = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')

    df_f = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"]).reset_index(drop=True)
    
    if not df_f.empty:
        regs = {}
        # Encabezados limpios (Sin puntos y comas)
        h1, h2, h3, h4, h5, h6 = st.columns([2.5, 1, 1, 1, 1, 1.2])
        with h1: st.write("**Insumo / Referencia**")
        with h2: st.write("**Almacén**")
        with h3: st.write("**Barra**")
        with h4: st.write("**Medida**")
        with h5: st.write("**Neto**")
        with h6: st.write("**¿Pedir?**")
        st.divider()

        for i, row in df_f.iterrows():
            nom = row['Nombre del Insumo']
            v_prev = 0.0
            if not ultimo_reg.empty:
                m = ultimo_reg[(ultimo_reg['Unidad de Negocio']==u_sel) & (ultimo_reg['Nombre del Insumo']==nom)]
                if not m.empty: v_prev = limpiar_valor(m.iloc[0].get('Stock Neto', 0.0))
            
            v_min = limpiar_valor(row.get('Stock Mínimo', 0))

            with st.container():
                c1, c2, c3, c4, c5, c6 = st.columns([2.5, 1, 1, 1, 1, 1.2])
                with c1:
                    st.write(f"**{nom}**")
                    diff = v_prev - v_min
                    color = "green" if diff > 0 else "red"
                    st.markdown(f"<small>Anterior: {v_prev} | Mín: {v_min} (<span style='color:{color}'>{diff:+.1f}</span>)</small>", unsafe_allow_html=True)
                with c2: v_a = st.number_input("Alm", min_value=0.0, key=f"a{i}", label_visibility="collapsed")
                with c3: v_b = st.number_input("Bar", min_value=0.0, key=f"b{i}", label_visibility="collapsed")
                with c4:
                    u_list = ["pz", "ml", "gr", "%", "kg", "lt"]
                    u_act = str(row.get("Unidad de Medida","pz")).lower()
                    v_u = st.selectbox("U", u_list, index=u_list.index(u_act) if u_act in u_list else 0, key=f"u{i}", label_visibility="collapsed")
                with c5:
                    v_n = v_a + v_b
                    st.write(f"**{v_n:.1f}**")
                with c6: v_p = st.toggle("🛒", key=f"p{i}")
                regs[nom] = {"a":v_a, "b":v_b, "n":v_n, "u":v_u, "p":v_p, "row":row}
            st.divider()

        if st.button("📥 SUBIR INVENTARIO A LA NUBE", use_container_width=True, type="primary"):
            filas = []
            fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for n, info in regs.items():
                dm = info["row"]
                filas.append([u_sel, n, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                              dm.get('Presentación de Compra',''), info["u"], info["a"], info["b"], 
                              info["n"], dm.get('Stock Mínimo',0), "TRUE" if info["p"] else "FALSE", r_sel, fh])
            sh.worksheet("Historial").append_rows(filas)
            st.cache_data.clear()
            st.success("¡Inventario guardado con éxito!")
            st.balloons()

# --- 7. PÁGINAS DE TICKETS (58MM) ---
elif st.session_state.pagina == "Impresion":
    st.title("🖨️ Ticket de Conteo (58mm)")
    u_sel = st.selectbox("Sucursal", ["Noble", "Coffee Station"])
    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    g_sel = st.multiselect("Filtrar Grupos", sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"])
    df_p = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"])
    if not df_p.empty:
        t = f"*** CONTEO {u_sel.upper()} ***\n" + "-"*22 + "\n"
        for _, r in df_p.iterrows(): t += f"[ ] {r['Nombre del Insumo'][:20]}\n"
        st.text_area("Copia para imprimir:", value=t + "-"*22, height=450)

elif st.session_state.pagina == "ListaCompra":
    st.title("🛒 Ticket de Compra (58mm)")
    u_sel = st.radio("Unidad:", ["Noble", "Coffee Station"], horizontal=True)
    if not df_historial.empty:
        ult = df_historial[df_historial['Unidad de Negocio']==u_sel].sort_values('Fecha de Inventario').drop_duplicates('Nombre del Insumo', keep='last')
        com = ult[ult['Necesita Compra'].astype(str).str.upper()=="TRUE"]
        if not com.empty:
            t = f"*** COMPRAS {u_sel.upper()} ***\n" + "-"*22 + "\n"
            for _, r in com.iterrows():
                t += f"• {r['Nombre del Insumo'][:18]}\n  Hay: {r['Stock Neto']} | Min: {r['Stock Mínimo']}\n\n"
            st.text_area("Copia para ticket de compra:", value=t + "-"*22, height=450)
        else: st.success("Todo en stock. No hay pedidos pendientes.")
