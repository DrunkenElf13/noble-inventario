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
    # Los secretos se configuran en el Dashboard de Streamlit
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
    client = gspread.authorize(creds)
    # Reemplaza con el ID de tu Sheets
    return client.open_by_key("1VZV81p-JqoaRPzMzsRurF6wntVefyaN5ozs3RJe6uJs")

try:
    sh = conectar_google_sheets()
    insumos_sheet = sh.worksheet("Insumos")
    historial_sheet = sh.worksheet("Historial")
except Exception as e:
    st.error("Error de conexión. Revisa los Secrets en Streamlit.")
    st.stop()

# 2. CARGA DE DATOS MAESTROS
# Traemos la pestaña Insumos para que el barista elija de la lista real
df_maestro = pd.DataFrame(insumos_sheet.get_all_records())

st.title("☕ Noble: Control de Inventario")
st.write("Registro de conteo físico diario")

# 3. FORMULARIO DE REGISTRO
with st.form("registro_noble"):
    # Selector de Insumo basado en tu Columna B
    insumo_nombre = st.selectbox("Selecciona el Insumo", df_maestro["Nombre del Insumo"].tolist())
    
    # Datos de entrada
    conteo_fisico = st.number_input("Cantidad física actual (Stock Neto)", min_value=0.0, step=1.0)
    responsable = st.selectbox("Responsable", ["Jenny", "José", "Araceli", "Raúl"])
    
    submit = st.form_submit_button("Registrar en Historial")

    if submit:
        # Buscamos la fila del insumo para heredar Marca, Proveedor, etc.
        datos_insumo = df_maestro[df_maestro["Nombre del Insumo"] == insumo_nombre].iloc[0]
        
        fecha_hoy = datetime.now().strftime("%Y-%m-%d")
        
        # Lógica de compra (booleano): Si el conteo es menor al mínimo, es True
        necesita_compra = "TRUE" if conteo_fisico <= datos_insumo.get("Stock Mínimo", 0) else "FALSE"

        # ESTRUCTURA DE 15 COLUMNAS (A a la O)
        # A: Unidad, B: Nombre, C: Marca, D: Proveedor, E: Grupo, F: FechaEntrada, G: PresCompra, 
        # H: UnidadMedida, I: Almacen, J: Activo, K: Neto (CONTEO), L: Minimo, M: Comprar, N: Resp, O: FechaInv
        nueva_fila = [
            str(datos_insumo.get("Unidad de Negocio", "Noble")), # A
            insumo_nombre,                                       # B
            str(datos_insumo.get("Marca", "")),                  # C
            str(datos_insumo.get("Proveedor", "")),              # D
            str(datos_insumo.get("Grupo", "")),                  # E
            str(datos_insumo.get("Fecha de Entrada", "")),       # F
            str(datos_insumo.get("Presentación de Compra", "")), # G
            str(datos_insumo.get("Unidad de Medida", "")),       # H
            0,                                                   # I (Almacén se queda en 0 o vacío en historial)
            0,                                                   # J (Stock Activo se queda en 0)
            conteo_fisico,                                       # K (ESTE ES EL DATO CLAVE)
            datos_insumo.get("Stock Mínimo", 0),                 # L
            necesita_compra,                                     # M
            responsable,                                         # N
            fecha_hoy                                            # O
        ]
        
        historial_sheet.append_row(nueva_fila)
        st.success(f"✅ {insumo_nombre} registrado con {conteo_fisico} unidades.")
        st.balloons()

# 4. MONITOR DE STOCK (ADMIN)
st.divider()
st.subheader("Estado Actual del Inventario")
df_historial = pd.DataFrame(historial_sheet.get_all_records())

if not df_historial.empty:
    # Mostramos solo lo relevante para que no sea una tabla gigante
    resumen = df_historial.sort_values("Fecha de Inventario").drop_duplicates("Nombre del Insumo", keep="last")
    st.dataframe(resumen[["Grupo", "Nombre del Insumo", "Stock Neto", "Responsable", "Fecha de Inventario"]])
