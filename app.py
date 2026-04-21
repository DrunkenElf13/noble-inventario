import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime

# CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Noble Inventario", page_icon="☕")

# 1. CONEXIÓN A GOOGLE SHEETS
def conectar_google_sheets():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
    client = gspread.authorize(creds)
    # EL ID DE TU HOJA (Asegúrate de que sea el tuyo)
    return client.open_by_key("1VZV81p-JqoaRPzMzsRurF6wntVefyaN5ozs3RJe6uJs")

try:
    sh = conectar_google_sheets()
    insumos_sheet = sh.worksheet("Insumos")
    historial_sheet = sh.worksheet("Historial")
except Exception as e:
    st.error("Error de conexión. Revisa los Secrets y que el Sheets esté compartido.")
    st.stop()

# 2. CARGA DE DATOS
df_maestro = pd.DataFrame(insumos_sheet.get_all_records())

st.title("☕ Noble: Control de Inventario")

# 3. FILTROS DE SELECCIÓN
st.subheader("Configuración de captura")
col1, col2 = st.columns(2)

with col1:
    responsable = st.selectbox("Responsable", ["Jenny", "José", "Araceli", "Raúl"])

with col2:
    # FILTRO POR GRUPO (Columna E)
    grupos_disponibles = sorted(df_maestro["Grupo"].unique().tolist())
    grupo_seleccionado = st.selectbox("Filtrar por Grupo", grupos_disponibles)

# Filtrar el dataframe según el grupo elegido
df_filtrado = df_maestro[df_maestro["Grupo"] == grupo_seleccionado]

st.divider()

# 4. FORMULARIO DE REGISTRO
with st.form("registro_noble"):
    # Ahora solo mostramos los insumos del grupo seleccionado
    insumo_nombre = st.selectbox("Selecciona el Insumo", df_filtrado["Nombre del Insumo"].tolist())
    
    conteo_fisico = st.number_input("Cantidad física actual (Stock Neto)", min_value=0.0, step=1.0)
    
    submit = st.form_submit_button("Registrar en Historial")

    if submit:
        datos_insumo = df_maestro[df_maestro["Nombre del Insumo"] == insumo_nombre].iloc[0]
        fecha_hoy = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Lógica de compra
        stock_min = datos_insumo.get("Stock Mínimo", 0)
        # Manejo de si el stock mínimo está vacío en el Excel
        try:
            stock_min = float(stock_min) if stock_min != "" else 0.0
        except:
            stock_min = 0.0
            
        necesita_compra = "TRUE" if conteo_fisico <= stock_min else "FALSE"

        # ESTRUCTURA DE 15 COLUMNAS (A a la O)
        nueva_fila = [
            str(datos_insumo.get("Unidad de Negocio", "Noble")), # A
            insumo_nombre,                                       # B
            str(datos_insumo.get("Marca", "")),                  # C
            str(datos_insumo.get("Proveedor", "")),              # D
            str(datos_insumo.get("Grupo", "")),                  # E
            str(datos_insumo.get("Fecha de Entrada", "")),       # F
            str(datos_insumo.get("Presentación de Compra", "")), # G
            str(datos_insumo.get("Unidad de Medida", "")),       # H
            0,                                                   # I
            0,                                                   # J
            conteo_fisico,                                       # K
            stock_min,                                           # L
            necesita_compra,                                     # M
            responsable,                                         # N
            fecha_hoy                                            # O
        ]
        
        historial_sheet.append_row(nueva_fila)
        st.success(f"✅ {insumo_nombre} ({grupo_seleccionado}) registrado con {conteo_fisico}.")
        st.balloons()

# 5. MONITOR DE STOCK (ADMIN) - Opcional, solo muestra los últimos de este grupo
st.divider()
st.subheader(f"Últimos conteos - Grupo {grupo_seleccionado}")
df_historial = pd.DataFrame(historial_sheet.get_all_records())

if not df_historial.empty:
    resumen = df_historial[df_historial["Grupo"] == grupo_seleccionado].sort_values("Fecha de Inventario").drop_duplicates("Nombre del Insumo", keep="last")
    st.dataframe(resumen[["Nombre del Insumo", "Stock Neto", "Responsable", "Fecha de Inventario"]])
