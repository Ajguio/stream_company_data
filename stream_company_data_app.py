import streamlit as st
import pandas as pd
import snowflake.connector
import os

# Título principal
st.title('Globant’s Data Engineering Coding Challenge')

# Encabezado del menú
st.header('DB migration with 3 different tables (departments, jobs, employees)')

# Elementos del menú
st.text('1. Receive historical data from CSV files')
st.text('2. Upload these files to the new DB')
st.text('3. Insert batch transactions (1 up to 1000 rows) with one request')

# Mapeo de tablas y encabezados
TABLE_HEADERS = {
    "departments.csv": ["id", "department"],
    "jobs.csv": ["id", "job"],
    "hired_employees.csv": ["id", "name", "datetime", "department_id", "job_id"]
}

# Conexión a Snowflake
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user='AJGFONSECA',
            password='Cu3nt4d3pr43bA;3',
            account='KWB93695',
            warehouse='compute_wh',
            database='company_data',
            schema='hiring_data'
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# Función para limpiar y preparar los datos
def clean_dataframe(dataframe, table_name):
    headers = TABLE_HEADERS[table_name]
    dataframe.columns = headers  # Asignar encabezados manualmente
    dataframe = dataframe.where(pd.notnull(dataframe), None)  # Reemplazar NaN con None
    return dataframe

# Función para insertar datos en la tabla correspondiente
def insert_data_to_snowflake(table_name, dataframe):
    try:
        conn = get_snowflake_connection()
        if conn is None:
            return False

        cursor = conn.cursor()

        # Limpiar datos antes de insertar
        dataframe = clean_dataframe(dataframe, table_name)

        # Generar consulta con marcadores de posición
        placeholders = ', '.join(['?'] * len(dataframe.columns))
        columns = ', '.join(dataframe.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Convertir DataFrame a lista de tuplas
        data = [tuple(row) for row in dataframe.to_numpy()]

        # Ejecutar la inserción en bloque
        cursor.executemany(sql, data)

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error inserting into {table_name}: {e}")
        return False

# Cargar archivos CSV desde la interfaz
st.header('Upload CSV Files')

uploaded_file = st.file_uploader("Upload a CSV file", type=['csv'])

if uploaded_file is not None:
    try:
        file_name = uploaded_file.name

        # Verificar si el archivo está mapeado
        if file_name not in TABLE_HEADERS:
            st.error("The file does not match any expected table.")
        else:
            # Leer el archivo sin encabezado
            dataframe = pd.read_csv(uploaded_file, header=None)

            # Determinar la tabla destino
            table_name = file_name.replace(".csv", "")

            st.write(f"Inserting data into table: {table_name}")
            success = insert_data_to_snowflake(table_name, dataframe)

            if success:
                st.success(f"Data successfully inserted into {table_name}.")
                st.dataframe(dataframe)
            else:
                st.error(f"Failed to insert data into {table_name}.")
    except Exception as e:
        st.error(f"Error processing the file: {e}")
