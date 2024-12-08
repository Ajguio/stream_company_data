import streamlit as st
import pandas as pd
import sqlite3  # O usa tu librería de conexión a la base de datos preferida

# Título principal
st.title('My Parents New Healthy Dinexxxxxxxxxxxxr')

# Encabezado del menú
st.header('Breakfast Menu')

# Elementos del menú
st.text('Omega 3 & Blueberry Oatmeal')
st.text('Kale, Spinach & Rocket Smoothie')
st.text('Hard-Boiled Free-Range Egg')

# Conexión a la base de datos (reemplaza con tu configuración)
def get_db_connection():
    # Conéctate a tu base de datos
    # Ejemplo: return psycopg2.connect("dbname=test user=postgres")
    conn = sqlite3.connect('example.db')  # Base de datos local para pruebas
    return conn

# Función para insertar datos en la tabla correspondiente
def insert_data_to_table(table_name, dataframe):
    try:
        conn = get_db_connection()
        dataframe.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error al insertar en la tabla {table_name}: {e}")
        return False

# Cargar archivos CSV desde la interfaz
st.header('Carga de Archivos CSV')

uploaded_file = st.file_uploader("Sube un archivo CSV", type=['csv'])

if uploaded_file is not None:
    # Leer el archivo subido
    try:
        # Detectar el nombre del archivo
        file_name = uploaded_file.name
        
        # Leer el contenido del CSV
        dataframe = pd.read_csv(uploaded_file)

        # Determinar la tabla destino según el nombre del archivo
        if file_name == 'departments.csv':
            table_name = 'departments'
        elif file_name == 'hired_employees.csv':
            table_name = 'hired_employees'
        elif file_name == 'jobs.csv':
            table_name = 'jobs'
        else:
            st.error("El archivo no coincide con ninguna tabla esperada.")
            table_name = None

        # Si se reconoce el archivo, insertar los datos en la tabla correspondiente
        if table_name:
            st.write(f"Insertando datos en la tabla: {table_name}")
            success = insert_data_to_table(table_name, dataframe)

            if success:
                st.success(f"Datos insertados correctamente en la tabla {table_name}.")
                st.dataframe(dataframe)
            else:
                st.error(f"No se pudo insertar en la tabla {table_name}.")
    except Exception as e:
        st.error(f"Error al procesar el archivo: {e}")
