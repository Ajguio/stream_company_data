import streamlit as st
import pandas as pd
import snowflake.connector

# Título principal
st.title('Globant’s Data Engineering Coding Challenge')

# Encabezado del menú
st.header('DB migration with 3 different tables (departments, jobs, employees')

# Elementos del menú
st.text('1. Receive historical data from CSV files')
st.text('2. Upload these files to the new DB')
st.text('3. Be able to insert batch transactions (1 up to 1000 rows) with one request You')

# Conexión a Snowflake
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user='YOUR_SNOWFLAKE_USER',
            password='YOUR_SNOWFLAKE_PASSWORD',
            account='YOUR_SNOWFLAKE_ACCOUNT',
            warehouse='YOUR_WAREHOUSE',
            database='YOUR_DATABASE',
            schema='YOUR_SCHEMA'
        )
        return conn
    except Exception as e:
        st.error(f"Error al conectar con Snowflake: {e}")
        return None

# Función para insertar datos en la tabla correspondiente
def insert_data_to_snowflake(table_name, dataframe):
    try:
        conn = get_snowflake_connection()
        if conn is None:
            return False

        # Crear un cursor y generar las sentencias de inserción
        cursor = conn.cursor()

        for _, row in dataframe.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(dataframe.columns)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))

        conn.commit()
        cursor.close()
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
            success = insert_data_to_snowflake(table_name, dataframe)

            if success:
