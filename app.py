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

# --- 2. FUNCIONES DE LIMPIEZA Y CARGA DE DATOS ---
def limpiar_valor(valor):
    """Limpia valores nulos o con símbolos (ej. 1050%) para evitar errores matemáticos."""
    if pd.isna(valor) or valor is None or valor == "": 
        return 0.0
    if isinstance(valor, (int, float)): 
        return float(valor)
    s = str(valor).replace('%', '').strip()
    try: 
        return float(s)
    except: 
        return 0.0

@st.cache_data(ttl=10)
def cargar_datos_integrales():
    if not sh: return pd.DataFrame(), pd.DataFrame()
    try:
        ins = pd.DataFrame(sh.worksheet("Insumos").get_all_records())
        his = pd.DataFrame(sh.worksheet("Historial").get_all_records())
        if not his.empty and 'Fecha de Inventario' in his.columns:
            his['Fecha de Inventario'] = pd.to_datetime(his['Fecha de Inventario'])
        return ins, his
    except Exception as e:
        st.warning(f"Aviso al cargar datos: {e}")
        return pd.DataFrame(), pd.DataFrame()

df_raw, df_historial = cargar_datos_integrales()

# --- 3. NAVEGACIÓN ---
if "pagina" not in st.session_state: 
    st.session_state.pagina = "Dashboard"

def cambiar_pagina(nombre):
    st.session_state.pagina = nombre
    st.rerun()

# --- 4. SIDEBAR INTEGRAL ---
with st.sidebar:
    st.title("⚙️ Operaciones Noble")
    if st.button("📊 Dashboard Principal", use_container_width=True): cambiar_pagina("Dashboard")
    
    st.divider()
    st.write("**📦 Movimientos de Stock:**")
    if st.button("📝 1. Conteo de Inventario", use_container_width=True): cambiar_pagina("Inventario")
    if st.button("📥 2. Ingreso de Compras", use_container_width=True): cambiar_pagina("Ingresos")
    
    st.divider()
    st.write("**🖨️ Tickets (58mm):**")
    if st.button("📋 1. Lista de Conteo", use_container_width=True): cambiar_pagina("Impresion")
    if st.button("🛒 2. Lista de Compra", use_container_width=True): cambiar_pagina("ListaCompra")
    
    st.divider()
    # Gestión de Equipo
    if "responsables" not in st.session_state:
        st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]
    
    with st.expander("👤 Equipo de Barra"):
        n_barista = st.text_input("Nuevo Barista:")
        if st.button("➕ Agregar Barista"):
            if n_barista: 
                st.session_state.responsables.append(n_barista)
                st.rerun()

    st.divider()
    # Gestión del Catálogo Completo
    st.subheader("🛠️ Gestión del Catálogo")
    op_cat = st.radio("Acción:", ["Añadir Insumo", "Editar Insumo"])

    if op_cat == "Añadir Insumo":
        with st.form("f_add", clear_on_submit=True):
            u = st.selectbox("Unidad", ["Noble", "Coffee Station"])
            n = st.text_input("Nombre del Insumo")
            m = st.text_input("Marca")
            p = st.text_input("Proveedor")
            g = st.selectbox("Grupo", ["A", "B", "C", "D", "E", "F"])
            uc = st.text_input("Presentación de Compra")
            um = st.selectbox("Unidad de Medida", ["pz", "ml", "gr", "%", "kg", "lt"])
            sm = st.number_input("Stock Mínimo", min_value=0.0)
            if st.form_submit_button("✨ Crear Insumo"):
                sh.worksheet("Insumos").append_row([u, n, m, p, g, "", uc, um, "", "", "", sm])
                st.cache_data.clear()
                st.success(f"Insumo {n} creado.")
                st.rerun()
    else:
        if not df_raw.empty and "Nombre del Insumo" in df_raw.columns:
            ins_edit = st.selectbox("Seleccionar Insumo a Editar:", df_raw["Nombre del Insumo"].tolist())
            d = df_raw[df_raw["Nombre del Insumo"] == ins_edit].iloc[0]
            with st.form("f_edit"):
                e_u = st.selectbox("Unidad", ["Noble", "Coffee Station"], index=0 if d.get("Unidad de Negocio")=="Noble" else 1)
                e_n = st.text_input("Nombre", value=str(d.get("Nombre del Insumo", "")))
                e_m = st.text_input("Marca", value=str(d.get("Marca", "")))
                e_p = st.text_input("Proveedor", value=str(d.get("Proveedor", "")))
                e_g = st.selectbox("Grupo", ["A","B","C","D","E","F"], index=["A","B","C","D","E","F"].index(d.get("Grupo","A")) if d.get("Grupo") in ["A","B","C","D","E","F"] else 0)
                e_uc = st.text_input("Presentación Compra", value=str(d.get("Presentación de Compra", "")))
                list_u = ["pz", "ml", "gr", "%", "kg", "lt"]
                u_val = str(d.get("Unidad de Medida", "pz")).lower()
                e_um = st.selectbox("Medida", list_u, index=list_u.index(u_val) if u_val in list_u else 0)
                e_sm = st.number_input("Stock Mínimo", value=limpiar_valor(d.get("Stock Mínimo", 0)))
                
                if st.form_submit_button("💾 Actualizar Insumo"):
                    idx = df_raw[df_raw["Nombre del Insumo"] == ins_edit].index[0] + 2
                    sh.worksheet("Insumos").update(range_name=f'A{idx}:L{idx}', values=[[e_u, e_n, e_m, e_p, e_g, "", e_uc, e_um, "", "", "", e_sm]])
                    st.cache_data.clear()
                    st.success("Catálogo actualizado.")
                    st.rerun()

# --- 5. PÁGINA: DASHBOARD ---
if st.session_state.pagina == "Dashboard":
    st.title("📊 Dashboard Operativo")
    
    if not df_historial.empty:
        try:
            ult = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
            crit = ult[ult['Necesita Compra'].astype(str).str.upper() == "TRUE"]
            
            c1, c2, c3 = st.columns(3)
            with c1: st.metric("🛒 Pendientes Noble", len(crit[crit['Unidad de Negocio']=="Noble"]))
            with c2: st.metric("🛒 Pendientes Coffee Station", len(crit[crit['Unidad de Negocio']=="Coffee Station"]))
            with c3: st.metric("🕒 Último Movimiento", df_historial['Fecha de Inventario'].max().strftime("%d/%m %H:%M"))
            
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("🏢 Faltantes: Noble")
                ins_n = crit[crit['Unidad de Negocio']=="Noble"]
                if not ins_n.empty:
                    for _, r in ins_n.iterrows():
                        st.error(f"**{r['Nombre del Insumo']}** (Stock Actual: {r['Stock Neto']} / Mínimo: {r['Stock Mínimo']})")
                else:
                    st.success("Noble está al día.")
            
            with col2:
                st.subheader("☕ Faltantes: Coffee Station")
                ins_cs = crit[crit['Unidad de Negocio']=="Coffee Station"]
                if not ins_cs.empty:
                    for _, r in ins_cs.iterrows():
                        st.error(f"**{r['Nombre del Insumo']}** (Stock Actual: {r['Stock Neto']} / Mínimo: {r['Stock Mínimo']})")
                else:
                    st.success("Coffee Station está al día.")
            
            st.divider()
            st.subheader("🕒 Actividad Reciente (Logs)")
            st.dataframe(df_historial.sort_values('Fecha de Inventario', ascending=False)[['Fecha de Inventario', 'Responsable', 'Unidad de Negocio', 'Nombre del Insumo', 'Stock Neto', 'Necesita Compra']].head(15), use_container_width=True)
        except Exception as e: 
            st.error(f"Error cargando los datos del Dashboard: {e}")
    else: 
        st.info("No hay datos históricos. Registra un inventario para ver las estadísticas.")

# --- 6. PÁGINA: INVENTARIO (CONTEO COMPLETO) ---
elif st.session_state.pagina == "Inventario":
    st.title("📝 Conteo de Inventario")
    
    col_u, col_r, col_g = st.columns([1, 1, 2])
    with col_u: u_sel = st.selectbox("🏢 Unidad de Negocio", ["Noble", "Coffee Station"])
    with col_r: r_sel = st.selectbox("👤 Responsable", st.session_state.responsables)
    with col_g:
        df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
        grps = sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty and "Grupo" in df_u.columns else ["A"]
        g_sel = st.multiselect("📂 Grupos a contar", grps, default=grps[:1])

    ultimo_reg = pd.DataFrame()
    if not df_historial.empty and 'Unidad de Negocio' in df_historial.columns:
        ultimo_reg = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')

    df_f = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"]).reset_index(drop=True)
    
    if not df_f.empty:
        regs = {}
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
                if not m.empty: 
                    v_prev = limpiar_valor(m.iloc[0].get('Stock Neto', 0.0))
            
            v_min = limpiar_valor(row.get('Stock Mínimo', 0))

            with st.container():
                c1, c2, c3, c4, c5, c6 = st.columns([2.5, 1, 1, 1, 1, 1.2])
                with c1:
                    st.write(f"**{nom}**")
                    st.caption(f"Marca: {row.get('Marca','-')} | Prov: {row.get('Proveedor','-')}")
                    diff = v_prev - v_min
                    color = "green" if diff > 0 else "red"
                    st.markdown(f"<small>Anterior: {v_prev} | Mín: {v_min} (<span style='color:{color}'>{diff:+.1f}</span>)</small>", unsafe_allow_html=True)
                
                with c2: v_a = st.number_input("Alm", min_value=0.0, step=1.0, key=f"a_{i}", label_visibility="collapsed")
                with c3: v_b = st.number_input("Bar", min_value=0.0, step=0.1, key=f"b_{i}", label_visibility="collapsed")
                
                with c4:
                    u_list = ["pz", "ml", "gr", "%", "kg", "lt"]
                    u_act = str(row.get("Unidad de Medida","pz")).lower()
                    v_u = st.selectbox("U", u_list, index=u_list.index(u_act) if u_act in u_list else 0, key=f"u_{i}", label_visibility="collapsed")
                
                with c5:
                    v_n = v_a + v_b
                    st.write(f"**{v_n:.1f}**")
                
                with c6: 
                    v_p = st.toggle("🛒", key=f"p_{i}")
                
                regs[nom] = {"a": v_a, "b": v_b, "n": v_n, "u": v_u, "p": v_p, "row": row}
            st.divider()

        if st.button("📥 SUBIR INVENTARIO", use_container_width=True, type="primary"):
            filas = []
            fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for n, info in regs.items():
                dm = info["row"]
                filas.append([u_sel, n, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                              dm.get('Presentación de Compra',''), info["u"], info["a"], info["b"], 
                              info["n"], dm.get('Stock Mínimo',0), "TRUE" if info["p"] else "FALSE", r_sel, fh])
            sh.worksheet("Historial").append_rows(filas)
            st.cache_data.clear()
            st.success("¡Inventario procesado con éxito!")
            st.balloons()

# --- 7. PÁGINA: INGRESOS DE COMPRAS (NUEVA) ---
elif st.session_state.pagina == "Ingresos":
    st.title("📥 Registro de Compras (Entradas)")
    st.info("Selecciona solo los insumos que llegaron. Las cantidades se sumarán automáticamente a tu stock de Almacén.")
    
    col_u, col_r = st.columns(2)
    with col_u: u_sel = st.selectbox("🏢 Unidad a ingresar:", ["Noble", "Coffee Station"])
    with col_r: r_sel = st.selectbox("👤 Responsable de recepción:", st.session_state.responsables)

    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    
    if not df_u.empty:
        insumos_llegados = st.multiselect("🔍 Busca y selecciona los insumos que vas a ingresar:", sorted(df_u["Nombre del Insumo"].tolist()))
        
        if insumos_llegados:
            ultimo_reg = pd.DataFrame()
            if not df_historial.empty and 'Unidad de Negocio' in df_historial.columns:
                ultimo_reg = df_historial.sort_values('Fecha de Inventario').drop_duplicates(['Unidad de Negocio', 'Nombre del Insumo'], keep='last')
            
            regs_ingreso = {}
            st.divider()
            
            h1, h2, h3, h4 = st.columns([3, 2, 2, 2])
            with h1: st.write("**Insumo**")
            with h2: st.write("**Stock Anterior (Alm + Bar)**")
            with h3: st.write("**+ Cantidad Ingresada**")
            with h4: st.write("**= Nuevo Stock Total**")
            st.divider()

            for i, nom in enumerate(insumos_llegados):
                row_insumo = df_u[df_u["Nombre del Insumo"] == nom].iloc[0]
                
                v_a_prev, v_b_prev = 0.0, 0.0
                if not ultimo_reg.empty:
                    m = ultimo_reg[(ultimo_reg['Unidad de Negocio']==u_sel) & (ultimo_reg['Nombre del Insumo']==nom)]
                    if not m.empty:
                        try: v_a_prev = limpiar_valor(m.iloc[0].get('Alm', 0.0))
                        except: pass
                        try: v_b_prev = limpiar_valor(m.iloc[0].get('Barra', 0.0))
                        except: pass
                
                v_n_prev = v_a_prev + v_b_prev
                v_min = limpiar_valor(row_insumo.get('Stock Mínimo', 0))
                
                with st.container():
                    c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                    with c1: 
                        st.write(f"**{nom}**")
                        st.caption(f"Medida: {row_insumo.get('Unidad de Medida', 'pz')}")
                    with c2: 
                        st.write(f"Almacén: {v_a_prev} | Barra: {v_b_prev}")
                        st.write(f"**Total Ant: {v_n_prev}**")
                    with c3: 
                        cant_ingreso = st.number_input("Ingreso", min_value=0.0, step=1.0, key=f"ing_{i}", label_visibility="collapsed")
                    with c4:
                        nuevo_alm = v_a_prev + cant_ingreso
                        nuevo_neto = nuevo_alm + v_b_prev
                        st.success(f"**{nuevo_neto:.1f}**")
                    
                    regs_ingreso[nom] = {
                        "nuevo_a": nuevo_alm, 
                        "b": v_b_prev, 
                        "nuevo_n": nuevo_neto, 
                        "row": row_insumo,
                        "min": v_min
                    }
                st.divider()

            if st.button("📦 REGISTRAR ENTRADAS", use_container_width=True, type="primary"):
                filas = []
                fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for n, info in regs_ingreso.items():
                    dm = info["row"]
                    # Calcula si aún necesita compra después del ingreso
                    necesita = "TRUE" if info["nuevo_n"] < info["min"] else "FALSE"
                    
                    filas.append([u_sel, n, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                                  dm.get('Presentación de Compra',''), dm.get('Unidad de Medida','pz'), 
                                  info["nuevo_a"], info["b"], info["nuevo_n"], info["min"], necesita, r_sel, fh])
                
                sh.worksheet("Historial").append_rows(filas)
                st.cache_data.clear()
                st.success("¡Compras registradas exitosamente en el Almacén!")
                st.balloons()

# --- 8. PÁGINAS DE TICKETS TÉRMICOS (58MM) ---
elif st.session_state.pagina == "Impresion":
    st.title("🖨️ Ticket de Conteo (Formato 58mm)")
    u_sel = st.selectbox("Sucursal a Contar:", ["Noble", "Coffee Station"])
    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    g_sel = st.multiselect("Filtrar por Grupos:", sorted(df_u["Grupo"].unique().tolist()) if not df_u.empty else ["A"])
    df_p = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"])
    
    if not df_p.empty:
        t = f"*** CONTEO {u_sel.upper()} ***\n"
        t += f"Resp: {st.session_state.responsables[0] if st.session_state.responsables else ''}\n"
        t += "-"*22 + "\n"
        for _, r in df_p.iterrows(): 
            t += f"[ ] {r['Nombre del Insumo'][:20]}\n"
        t += "-"*22 + "\n"
        st.text_area("Copia este texto para tu impresora térmica:", value=t, height=450)
    else:
        st.warning("Selecciona al menos un grupo para generar el ticket.")

elif st.session_state.pagina == "ListaCompra":
    st.title("🛒 Ticket de Compra (Formato 58mm)")
    u_sel = st.radio("Generar lista para:", ["Noble", "Coffee Station"], horizontal=True)
    
    if not df_historial.empty:
        ult = df_historial[df_historial['Unidad de Negocio']==u_sel].sort_values('Fecha de Inventario').drop_duplicates('Nombre del Insumo', keep='last')
        com = ult[ult['Necesita Compra'].astype(str).str.upper()=="TRUE"]
        
        if not com.empty:
            t = f"*** COMPRAS {u_sel.upper()} ***\n"
            t += f"Fecha: {datetime.now().strftime('%d/%m/%Y')}\n"
            t += "-"*22 + "\n"
            for _, r in com.iterrows():
                t += f"• {r['Nombre del Insumo'][:18]}\n"
                t += f"  Hay: {r['Stock Neto']} | Min: {r['Stock Mínimo']}\n\n"
            t += "-"*22 + "\n"
            st.text_area("Copia este texto para llevarlo de compras:", value=t, height=450)
        else: 
            st.success("¡Todo está en orden! No hay pedidos pendientes marcados en el último inventario.")
