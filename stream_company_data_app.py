pip install snowflake-connector-python

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

# Conexión a Snowflake
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# Función para insertar datos en la tabla correspondiente
def insert_data_to_snowflake(table_name, dataframe):
    try:
        conn = get_snowflake_connection()
        if conn is None:
            return False

        cursor = conn.cursor()
        columns = ', '.join(dataframe.columns)
        placeholders = ', '.join(['?'] * len(dataframe.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Insertar en bloque
        cursor.executemany(sql, dataframe.values.tolist())

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
        dataframe = pd.read_csv(uploaded_file)

        # Determinar la tabla destino
        if file_name == 'departments.csv':
            table_name = 'departments'
        elif file_name == 'hired_employees.csv':
            table_name = 'hired_employees'
        elif file_name == 'jobs.csv':
            table_name = 'jobs'
        else:
            st.error("The file does not match any expected table.")
            table_name = None

        if table_name:
            st.write(f"Inserting data into table: {table_name}")
            success = insert_data_to_snowflake(table_name, dataframe)

            if success:
                st.success(f"Data successfully inserted into {table_name}.")
                st.dataframe(dataframe)
            else:
                st.error(f"Failed to insert data into {table_name}.")
    except Exception as e:
        st.error(f"Error processing the file: {e}")
