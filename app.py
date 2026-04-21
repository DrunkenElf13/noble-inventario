import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime
import time

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
        st.error(f"Error crítico de conexión con base de datos: {e}")
        return None

sh = conectar_google_sheets()

# --- 2. CAPA DE LIMPIEZA Y NORMALIZACIÓN DE DATOS ---
def limpiar_valor(valor):
    """Limpia valores nulos o con símbolos para evitar excepciones de tipo flotante."""
    if pd.isna(valor) or valor is None or str(valor).strip() == "": 
        return 0.0
    if isinstance(valor, (int, float)): 
        return float(valor)
    try:
        # Remueve símbolos comunes que rompen cálculos numéricos
        s = str(valor).replace('%', '').replace('$', '').replace(',', '').strip()
        return float(s)
    except Exception: 
        return 0.0

def normalizar_dataframe(df, columnas_esperadas):
    """
    WRAPPER MEJORADO: Garantiza que el DF tenga las columnas mapeadas por POSICIÓN exacta.
    Ignora por completo cómo se llaman los encabezados en Google Sheets para evitar el bug de '0.0'.
    """
    if df.empty:
        return pd.DataFrame(columns=columnas_esperadas)
    
    # 1. Mapeo posicional estricto: forzamos el nombre de la columna según su índice real
    columnas_actuales = list(df.columns)
    nuevos_nombres = []
    
    for i in range(len(columnas_actuales)):
        if i < len(columnas_esperadas):
            nuevos_nombres.append(columnas_esperadas[i])
        else:
            nuevos_nombres.append(columnas_actuales[i]) # Mantiene data extra sin romper
            
    df.columns = nuevos_nombres
    
    # 2. Inyectar columnas faltantes para evitar KeyErrors silenciosos al final de la matriz
    for col in columnas_esperadas:
        if col not in df.columns:
            df[col] = None 
            
    return df

@st.cache_data(ttl=15)
def cargar_datos_integrales():
    """Descarga, limpia y estandariza los datos de la base leyendo la matriz cruda."""
    if not sh: return pd.DataFrame(), pd.DataFrame()
    
    try:
        # EXTRACCIÓN SEGURA: get_all_values() trae matriz pura, ignorando fallos de encabezados en Sheets
        val_ins = sh.worksheet("Insumos").get_all_values()
        val_his = sh.worksheet("Historial").get_all_values()
        
        if len(val_ins) > 1:
            df_ins = pd.DataFrame(val_ins[1:], columns=val_ins[0])
        else:
            df_ins = pd.DataFrame(columns=val_ins[0] if val_ins else [])
            
        if len(val_his) > 1:
            df_his = pd.DataFrame(val_his[1:], columns=val_his[0])
        else:
            df_his = pd.DataFrame(columns=val_his[0] if val_his else [])
            
        # ANCLA DE SEGURIDAD: Guardamos la fila real de Google Sheets antes de limpiar el DF
        df_ins['Sheet_Row_Num'] = df_ins.index + 2
        
        # PLANTILLAS ESTRICTAS DE POSICIÓN (Basado en el orden exacto del append de la app)
        cols_insumos = ['Unidad de Negocio', 'Nombre del Insumo', 'Marca', 'Proveedor', 'Grupo', 'Espacio_1', 'Presentación de Compra', 'Unidad de Medida', 'Espacio_2', 'Espacio_3', 'Espacio_4', 'Stock Mínimo']
        cols_historial = ['Unidad de Negocio', 'Nombre del Insumo', 'Marca_H', 'Proveedor_H', 'Grupo_H', 'Espacio_H', 'Pres_Compra_H', 'Unidad_Medida_H', 'Alm', 'Barra', 'Stock Neto', 'Stock Mínimo', 'Necesita Compra', 'Responsable', 'Fecha de Inventario']
        
        df_ins = normalizar_dataframe(df_ins, cols_insumos)
        df_his = normalizar_dataframe(df_his, cols_historial)
        
        # Parseo seguro de fechas para ordenamiento cronológico
        if not df_his.empty:
            df_his['Fecha de Inventario'] = pd.to_datetime(df_his['Fecha de Inventario'], errors='coerce')
            
        return df_ins, df_his
        
    except Exception as e:
        st.error(f"Falla en la extracción de datos: {e}")
        return pd.DataFrame(), pd.DataFrame()

# Carga inicial global
df_raw, df_historial = cargar_datos_integrales()

# --- 3. LÓGICA DE NEGOCIO ---
def obtener_ultimo_inventario(df_hist, unidad=None):
    """Devuelve una foto del último stock registrado por insumo."""
    if df_hist.empty: return pd.DataFrame()
    
    df_u = df_hist.copy()
    if unidad:
        df_u = df_u[df_u['Unidad de Negocio'] == unidad]
        
    if df_u.empty: return pd.DataFrame()
    
    # Ordenar cronológicamente y mantener solo el registro más reciente por Insumo y Unidad
    df_actual = df_u.sort_values('Fecha de Inventario', ascending=True, na_position='first').drop_duplicates(subset=['Unidad de Negocio', 'Nombre del Insumo'], keep='last').copy()
    
    # Estandarización matemática estricta
    for col in ['Alm', 'Barra', 'Stock Neto', 'Stock Mínimo']:
        df_actual[col] = df_actual[col].apply(limpiar_valor)
        
    df_actual['Stock Neto Calculado'] = df_actual['Alm'] + df_actual['Barra']
    df_actual['Necesita Compra'] = df_actual['Stock Neto Calculado'] < df_actual['Stock Mínimo']
    
    return df_actual

# --- 4. RUTEO Y ESTADO DE SESIÓN ---
if "pagina" not in st.session_state: 
    st.session_state.pagina = "Dashboard"
if "responsables" not in st.session_state:
    st.session_state.responsables = ["Jenny", "Araceli", "Raúl"]

def cambiar_pagina(nombre):
    st.session_state.pagina = nombre
    st.rerun()

# --- 5. INTERFAZ: SIDEBAR ---
with st.sidebar:
    st.title("⚙️ Operaciones Noble")
    if st.button("📊 Dashboard Principal", use_container_width=True): cambiar_pagina("Dashboard")
    
    st.divider()
    st.write("**📦 Movimientos de Stock:**")
    if st.button("📝 1. Conteo de Inventario", use_container_width=True): cambiar_pagina("Inventario")
    if st.button("📥 2. Ingreso de Compras", use_container_width=True): cambiar_pagina("Ingresos")
    if st.button("📦 3. Ver Inventario Actual", use_container_width=True): cambiar_pagina("Consulta")
    
    st.divider()
    st.write("**🖨️ Tickets (58mm):**")
    if st.button("📋 1. Lista de Conteo", use_container_width=True): cambiar_pagina("Impresion")
    if st.button("🛒 2. Lista de Compra", use_container_width=True): cambiar_pagina("ListaCompra")
    
    st.divider()
    with st.expander("👤 Equipo de Barra"):
        n_barista = st.text_input("Nuevo Barista:")
        if st.button("➕ Agregar Barista") and n_barista:
            st.session_state.responsables.append(n_barista)
            st.rerun()

    st.divider()
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
                try:
                    nueva_fila = [u, n, m, p, g, "", uc, um, "", "", "", sm]
                    sh.worksheet("Insumos").append_row(nueva_fila)
                    st.cache_data.clear()
                    st.success(f"Insumo '{n}' integrado al sistema.")
                    time.sleep(1) 
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al conectar con la base maestra: {e}")
                
    elif not df_raw.empty and "Nombre del Insumo" in df_raw.columns:
        ins_nombres = df_raw["Nombre del Insumo"].dropna().unique().tolist()
        if ins_nombres:
            ins_edit = st.selectbox("Seleccionar Insumo a Editar:", ins_nombres)
            d = df_raw[df_raw["Nombre del Insumo"] == ins_edit].iloc[0]
            
            with st.form("f_edit"):
                e_u = st.selectbox("Unidad", ["Noble", "Coffee Station"], index=0 if d.get("Unidad de Negocio")=="Noble" else 1)
                e_n = st.text_input("Nombre", value=str(d.get("Nombre del Insumo", "")))
                e_m = st.text_input("Marca", value=str(d.get("Marca", "")))
                e_p = st.text_input("Proveedor", value=str(d.get("Proveedor", "")))
                
                grupo_val = str(d.get("Grupo", "A"))
                e_g = st.selectbox("Grupo", ["A","B","C","D","E","F"], index=["A","B","C","D","E","F"].index(grupo_val) if grupo_val in ["A","B","C","D","E","F"] else 0)
                
                e_uc = st.text_input("Presentación Compra", value=str(d.get("Presentación de Compra", "")))
                
                list_u = ["pz", "ml", "gr", "%", "kg", "lt"]
                u_val = str(d.get("Unidad de Medida", "pz")).lower()
                e_um = st.selectbox("Medida", list_u, index=list_u.index(u_val) if u_val in list_u else 0)
                
                e_sm = st.number_input("Stock Mínimo", value=limpiar_valor(d.get("Stock Mínimo", 0)))
                
                if st.form_submit_button("💾 Actualizar Insumo"):
                    try:
                        # Uso de ancla segura de índice para evitar fallos si se han filtrado valores vacíos
                        idx = int(d.get('Sheet_Row_Num', df_raw[df_raw["Nombre del Insumo"] == ins_edit].index[0] + 2))
                        fila_act = [[e_u, e_n, e_m, e_p, e_g, "", e_uc, e_um, "", "", "", e_sm]]
                        sh.worksheet("Insumos").update(range_name=f'A{idx}:L{idx}', values=fila_act)
                        st.cache_data.clear()
                        st.success("Catálogo sincronizado.")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al sincronizar catálogo: {e}")

# --- 6. VISTAS PRINCIPALES ---

if st.session_state.pagina == "Dashboard":
    st.title("📊 Dashboard Operativo")
    
    df_actual = obtener_ultimo_inventario(df_historial)
    
    if not df_actual.empty:
        crit = df_actual[df_actual['Necesita Compra'] == True]
        
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("🛒 Pendientes Noble", len(crit[crit['Unidad de Negocio']=="Noble"]))
        with c2: st.metric("🛒 Pendientes Coffee Station", len(crit[crit['Unidad de Negocio']=="Coffee Station"]))
        with c3: st.metric("🕒 Último Movimiento", df_actual['Fecha de Inventario'].max().strftime("%d/%m %H:%M"))
        
        st.divider()
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏢 Faltantes: Noble")
            ins_n = crit[crit['Unidad de Negocio']=="Noble"]
            if not ins_n.empty:
                for _, r in ins_n.iterrows():
                    st.error(f"**{r['Nombre del Insumo']}** (Stock: {r['Stock Neto Calculado']} / Mín: {r['Stock Mínimo']})")
            else:
                st.success("Operación cubierta.")
                
        with col2:
            st.subheader("☕ Faltantes: Coffee Station")
            ins_cs = crit[crit['Unidad de Negocio']=="Coffee Station"]
            if not ins_cs.empty:
                for _, r in ins_cs.iterrows():
                    st.error(f"**{r['Nombre del Insumo']}** (Stock: {r['Stock Neto Calculado']} / Mín: {r['Stock Mínimo']})")
            else:
                st.success("Operación cubierta.")
        
        st.divider()
        st.subheader("🕒 Actividad Reciente (Logs)")
        logs_mostrar = df_historial.dropna(subset=['Fecha de Inventario']).sort_values('Fecha de Inventario', ascending=False)
        st.dataframe(logs_mostrar[['Fecha de Inventario', 'Responsable', 'Unidad de Negocio', 'Nombre del Insumo', 'Stock Neto', 'Necesita Compra']].head(15), use_container_width=True)
    else: 
        st.info("Sin datos históricos. Ejecuta el primer conteo de inventario.")

elif st.session_state.pagina == "Inventario":
    st.title("📝 Conteo de Inventario")
    
    col_u, col_r, col_g = st.columns([1, 1, 2])
    with col_u: u_sel = st.selectbox("🏢 Unidad de Negocio", ["Noble", "Coffee Station"])
    with col_r: r_sel = st.selectbox("👤 Responsable", st.session_state.responsables)
    
    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    
    with col_g:
        grps = sorted(df_u["Grupo"].dropna().unique().tolist()) if not df_u.empty and "Grupo" in df_u.columns else ["A"]
        g_sel = st.multiselect("📂 Grupos a contar", grps, default=grps[:1] if grps else [])

    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    df_f = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"]).reset_index(drop=True)
    
    if not df_f.empty:
        regs = {}
        h1, h2, h3, h4, h5, h6 = st.columns([2.5, 1, 1, 1, 1, 1.2])
        with h1: st.write("**Insumo / Ref**")
        with h2: st.write("**Almacén**")
        with h3: st.write("**Barra**")
        with h4: st.write("**Medida**")
        with h5: st.write("**Neto**")
        with h6: st.write("**¿Pedir?**")
        st.divider()

        for i, row in df_f.iterrows():
            nom = str(row.get('Nombre del Insumo', ''))
            v_prev = 0.0
            
            if not df_actual.empty:
                match = df_actual[df_actual['Nombre del Insumo'] == nom]
                if not match.empty:
                    v_prev = match.iloc[0]['Stock Neto Calculado']
            
            v_min = limpiar_valor(row.get('Stock Mínimo', 0))

            with st.container():
                c1, c2, c3, c4, c5, c6 = st.columns([2.5, 1, 1, 1, 1, 1.2])
                with c1:
                    st.write(f"**{nom}**")
                    st.caption(f"Prov: {row.get('Proveedor','-')}")
                    diff = v_prev - v_min
                    color = "green" if diff >= 0 else "red"
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
                    auto_pedir = bool(v_n < v_min)
                    v_p = st.toggle("🛒", value=auto_pedir, key=f"p_{i}")
                
                regs[nom] = {"a": v_a, "b": v_b, "n": v_n, "u": v_u, "p": v_p, "row": row}
            st.divider()

        if st.button("📥 PROCESAR INVENTARIO", use_container_width=True, type="primary"):
            filas = []
            fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for n, info in regs.items():
                dm = info["row"]
                filas.append([
                    u_sel, n, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                    dm.get('Presentación de Compra',''), info["u"], info["a"], info["b"], 
                    info["n"], dm.get('Stock Mínimo',0), "TRUE" if info["p"] else "FALSE", r_sel, fh
                ])
            try:
                sh.worksheet("Historial").append_rows(filas)
                st.cache_data.clear()
                st.success("¡Transacción exitosa! Inventario actualizado.")
                time.sleep(0.5)
                st.rerun() 
            except Exception as e:
                st.error(f"Falla al escribir en base de datos: {e}")

elif st.session_state.pagina == "Ingresos":
    st.title("📥 Registro de Compras (Entradas)")
    st.info("Ingresa insumos recibidos. Se sumarán a tu último corte de Almacén.")
    
    col_u, col_r = st.columns(2)
    with col_u: u_sel = st.selectbox("🏢 Unidad receptora:", ["Noble", "Coffee Station"])
    with col_r: r_sel = st.selectbox("👤 Responsable:", st.session_state.responsables)

    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    
    if not df_u.empty:
        nombres_insumos = df_u["Nombre del Insumo"].dropna().unique().tolist()
        insumos_llegados = st.multiselect("🔍 Selecciona insumos que llegaron:", sorted(nombres_insumos))
        
        if insumos_llegados:
            df_actual = obtener_ultimo_inventario(df_historial, u_sel)
            regs_ingreso = {}
            st.divider()
            
            h1, h2, h3, h4 = st.columns([3, 2, 2, 2])
            with h1: st.write("**Insumo**")
            with h2: st.write("**Stock Anterior (Alm+Bar)**")
            with h3: st.write("**+ Cantidad Ingresada**")
            with h4: st.write("**= Nuevo Stock Total**")
            st.divider()

            for i, nom in enumerate(insumos_llegados):
                row_insumo = df_u[df_u["Nombre del Insumo"] == nom].iloc[0]
                
                v_a_prev, v_b_prev = 0.0, 0.0
                if not df_actual.empty:
                    m = df_actual[df_actual['Nombre del Insumo'] == nom]
                    if not m.empty:
                        v_a_prev = m.iloc[0]['Alm']
                        v_b_prev = m.iloc[0]['Barra']
                
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
                        cant_ingreso = st.number_input("Ingreso a Almacén", min_value=0.0, step=1.0, key=f"ing_{i}", label_visibility="collapsed")
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

            if st.button("📦 EJECUTAR INGRESO", use_container_width=True, type="primary"):
                filas = []
                fh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for n, info in regs_ingreso.items():
                    dm = info["row"]
                    necesita = "TRUE" if info["nuevo_n"] < info["min"] else "FALSE"
                    filas.append([
                        u_sel, n, dm.get('Marca',''), dm.get('Proveedor',''), dm.get('Grupo',''), "", 
                        dm.get('Presentación de Compra',''), dm.get('Unidad de Medida','pz'), 
                        info["nuevo_a"], info["b"], info["nuevo_n"], info["min"], necesita, r_sel, fh
                    ])
                try:
                    sh.worksheet("Historial").append_rows(filas)
                    st.cache_data.clear()
                    st.success("Ingreso registrado de manera exitosa.")
                    time.sleep(0.5)
                    st.rerun() 
                except Exception as e:
                    st.error(f"Falla al registrar ingresos: {e}")

elif st.session_state.pagina == "Consulta":
    st.title("📦 Auditoría de Inventario Actual")
    u_sel = st.selectbox("🏢 Unidad:", ["Noble", "Coffee Station"])
    
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    if not df_actual.empty:
        bajo_min = df_actual[df_actual['Necesita Compra'] == True]
        
        m1, m2, m3 = st.columns(3)
        with m1: st.metric("Total Referencias", len(df_actual))
        with m2: st.metric("Alertas Bajo Mínimo", len(bajo_min), delta=-len(bajo_min), delta_color="inverse")
        with m3: st.metric("Volumen Global", f"{df_actual['Stock Neto Calculado'].sum():,.1f}")
        
        st.divider()
        col_search, col_prov = st.columns([2, 1])
        with col_search: busqueda = st.text_input("🔍 Búsqueda rápida:")
        with col_prov:
            provs = ["Todos"] + sorted(df_actual['Proveedor'].dropna().unique().tolist())
            prov_sel = st.selectbox("🚛 Filtro Proveedor:", provs)
        
        df_display = df_actual.copy()
        if busqueda: df_display = df_display[df_display['Nombre del Insumo'].astype(str).str.contains(busqueda, case=False, na=False)]
        if prov_sel != "Todos": df_display = df_display[df_display['Proveedor'] == prov_sel]
        
        st.subheader(f"Estatus de Flujo - {u_sel}")
        df_final = df_display[['Grupo', 'Nombre del Insumo', 'Marca', 'Proveedor', 'Alm', 'Barra', 'Stock Neto Calculado', 'Unidad de Medida', 'Stock Mínimo', 'Fecha de Inventario']].copy()
        df_final.columns = ['Grupo', 'Insumo', 'Marca', 'Proveedor', 'Almacén', 'Barra', 'Stock Total', 'Medida', 'Mínimo', 'Último Corte']
        
        def highlight_low_stock(s):
            return ['background-color: rgba(255, 75, 75, 0.2)' if (s['Stock Total'] < s['Mínimo']) else '' for _ in s]

        st.dataframe(df_final.style.apply(highlight_low_stock, axis=1), use_container_width=True, hide_index=True)
    else:
        st.warning("No hay registros en la base de datos para ejecutar la auditoría.")

elif st.session_state.pagina == "Impresion":
    st.title("🖨️ Logística: Ticket de Conteo")
    u_sel = st.selectbox("Sucursal:", ["Noble", "Coffee Station"])
    df_u = df_raw[df_raw["Unidad de Negocio"] == u_sel] if not df_raw.empty else pd.DataFrame()
    
    grps = sorted(df_u["Grupo"].dropna().unique().tolist()) if not df_u.empty and "Grupo" in df_u.columns else []
    g_sel = st.multiselect("Filtrar por Grupos:", grps)
    
    if g_sel and not df_u.empty:
        df_p = df_u[df_u["Grupo"].isin(g_sel)].sort_values(["Grupo", "Nombre del Insumo"])
        t = f"*** CONTEO {u_sel.upper()} ***\n"
        t += f"Resp: {st.session_state.responsables[0] if st.session_state.responsables else ''}\n"
        t += "-"*22 + "\n"
        for _, r in df_p.iterrows(): 
            t += f"[ ] {r['Nombre del Insumo'][:20]}\n"
        t += "-"*22 + "\n"
        st.text_area("Formato Térmico (58mm):", value=t, height=450)
    else:
        st.info("Selecciona grupos operativos para generar la lista.")

elif st.session_state.pagina == "ListaCompra":
    st.title("🛒 Logística: Ticket de Compra")
    u_sel = st.radio("Generar orden para:", ["Noble", "Coffee Station"], horizontal=True)
    
    df_actual = obtener_ultimo_inventario(df_historial, u_sel)
    if not df_actual.empty:
        com = df_actual[df_actual['Necesita Compra'] == True]
        if not com.empty:
            t = f"*** COMPRAS {u_sel.upper()} ***\n"
            t += f"Fecha: {datetime.now().strftime('%d/%m/%Y')}\n"
            t += "-"*22 + "\n"
            for _, r in com.iterrows():
                t += f"• {str(r['Nombre del Insumo'])[:18]}\n"
                t += f"  Hay: {r['Stock Neto Calculado']} | Min: {r['Stock Mínimo']}\n\n"
            t += "-"*22 + "\n"
            st.text_area("Formato Térmico (58mm):", value=t, height=450)
        else: 
            st.success("No se han disparado alertas de reabastecimiento.")
    else:
        st.info("Sin registros suficientes para armar la logística de compra.")
