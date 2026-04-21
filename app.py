import streamlit as st
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
from datetime import datetime

# CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Noble Inventario", page_icon="☕", layout="wide")

# 1. CONEXIÓN A GOOGLE SHEETS
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
df_maestro = pd.DataFrame(insumos_sheet.get_all_records())

st.title("☕ Noble: Control de Inventario")

# 3. SELECCIÓN DE CONTEXTO
col_a, col_b = st.columns(2)
with col_a:
    responsable = st.selectbox("Responsable", ["Jenny", "José", "Araceli", "Raúl"])
with col_b:
    grupos_disponibles = sorted(df_maestro["Grupo"].unique().tolist())
    grupo_sel = st.selectbox("Selecciona Grupo para Inventariar", grupos_disponibles)

st.divider()

# 4. DESPLIEGUE POR GRUPO
df_filtrado = df_maestro[df_maestro["Grupo"] == grupo_sel].reset_index(drop=True)

if not df_filtrado.empty:
    st.subheader(f"Lista de Insumos: Grupo {grupo_sel}")
    st.info("Ingresa las cantidades de los productos que desees registrar.")

    # Diccionario para guardar las entradas de usuario
    nuevos_registros = {}

    # Generar campos de entrada para cada insumo del grupo
    for index, row in df_filtrado.iterrows():
        nombre = row["Nombre del Insumo"]
        # Usamos columnas para que se vea ordenado: Nombre | Input
        c1, c2 = st.columns([3, 1])
        with c1:
            st.write(f"**{nombre}**")
            st.caption(f"Marca: {row.get('Marca', 'N/A')} | Stock Mín: {row.get('Stock Mínimo', 0)}")
        with c2:
            # El valor por defecto es None para saber si el usuario escribió algo
            cantidad = st.number_input("Cantidad", min_value=0.0, step=1.0, key=f"input_{index}", value=0.0)
            nuevos_registros[nombre] = {
                "cantidad": cantidad,
                "datos_maestros": row
            }
        st.divider()

    # 5. BOTÓN DE PROCESAMIENTO MASIVO
    if st.button(f"🚀 Registrar Inventario Grupo {grupo_sel}", use_container_width=True):
        fecha_hoy = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filas_para_excel = []

        for nombre, info in nuevos_registros.items():
            cant = info["cantidad"]
            dm = info["datos_maestros"]
            
            # Solo registramos si la cantidad es mayor a 0 (o puedes quitar esta validación si quieres registrar ceros)
            stock_min = 0.0
            try:
                stock_min = float(dm.get("Stock Mínimo", 0))
            except:
                stock_min = 0.0
            
            necesita_compra = "TRUE" if cant <= stock_min else "FALSE"

            fila = [
                str(dm.get("Unidad de Negocio", "Noble")), # A
                nombre,                                     # B
                str(dm.get("Marca", "")),                  # C
                str(dm.get("Proveedor", "")),              # D
                str(dm.get("Grupo", "")),                  # E
                str(dm.get("Fecha de Entrada", "")),       # F
                str(dm.get("Presentación de Compra", "")), # G
                str(dm.get("Unidad de Medida", "")),       # H
                0,                                         # I
                0,                                         # J
                cant,                                      # K
                stock_min,                                 # L
                necesita_compra,                           # M
                responsable,                               # N
                fecha_hoy                                  # O
            ]
            filas_para_excel.append(fila)

        if filas_para_excel:
            historial_sheet.append_rows(filas_para_excel)
            st.success(f"✅ Se han registrado {len(filas_para_excel)} insumos del Grupo {grupo_sel} exitosamente.")
            st.balloons()
else:
    st.warning(f"No se encontraron insumos en el Grupo {grupo_sel}.")
