import streamlit as st
import pandas as pd
import snowflake.connector

# Mapeo de archivos a tablas y encabezados
FILE_TABLE_MAPPING = {
    "departments.csv": "departments",
    "jobs.csv": "jobs",
    "hired_employees.csv": "hired_employees"
}

TABLE_HEADERS = {
    "departments.csv": ["id", "department"],
    "jobs.csv": ["id", "job"],
    "hired_employees.csv": ["id", "name", "datetime", "department_id", "job_id"]
}

# Título principal
st.title('Globant’s Data Engineering Coding Challenge')

# Encabezado del menú
st.header('DB migration with 3 different tables (departments, jobs, employees')

# Elementos del menú
st.text('1. Receive historical data from CSV files')
st.text('2. Upload these files to the new DB')
st.text('3. Be able to insert batch transactions (1 up to 1000 rows) with one request You')


# Conexión a Snowflake usando streamlit.secrets
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(**st.secrets["snowflake"])
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# Función para limpiar y preparar los datos
def clean_dataframe(dataframe, file_name):
    headers = TABLE_HEADERS[file_name]
    dataframe.columns = headers  # Asignar encabezados manualmente

    # Convertir NaN y valores vacíos a None
    dataframe = dataframe.astype(object).where(pd.notnull(dataframe), None)

    # Manejar específicamente la columna datetime
    if "datetime" in dataframe.columns:
        dataframe["datetime"] = pd.to_datetime(
            dataframe["datetime"], errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S").where(pd.notnull(dataframe["datetime"]), None)

    return dataframe

# Función para insertar datos en la tabla correspondiente
def insert_data_to_snowflake(table_name, dataframe):
    try:
        conn = get_snowflake_connection()
        if conn is None:
            return False

        cursor = conn.cursor()

        # Insertar el DataFrame directamente usando Snowflake
        for _, row in dataframe.iterrows():
            values = tuple(row)
            placeholders = ', '.join(['%s'] * len(row))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

            # Ejecutar el insert
            cursor.execute(sql, values)

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error inserting into {table_name}: {e}")
        return False

# Cargar archivos CSV desde la interfaz
st.header('Upload CSV Files')

uploaded_file = st.file_uploader("Upload a CSV file", type=['csv'], key="file_uploader_unique")

if uploaded_file is not None:
    try:
        file_name = uploaded_file.name

        # Verificar si el archivo está mapeado
        if file_name not in FILE_TABLE_MAPPING:
            st.error("The file does not match any expected table.")
        else:
            # Leer el archivo sin encabezado
            dataframe = pd.read_csv(uploaded_file, header=None)

            # Mostrar contenido del DataFrame cargado
            st.write("Uploaded DataFrame:")
            st.dataframe(dataframe)

            # Determinar la tabla destino
            table_name = FILE_TABLE_MAPPING[file_name]

            # Limpiar el DataFrame
            dataframe = clean_dataframe(dataframe, file_name)

            st.write(f"Inserting data into table: {table_name}")
            success = insert_data_to_snowflake(table_name, dataframe)

            if success:
                st.success(f"Data successfully inserted into {table_name}.")
            else:
                st.error(f"Failed to insert data into {table_name}.")
    except Exception as e:
        st.error(f"Error processing the file: {e}")

# Cargar archivos CSV desde la interfaz
st.header('Generate Report')

year = st.text_input('Enter the year for the reports:', key="year_input")

if st.button('Generate Information'):
    if not year.isdigit():
        st.error("Please enter a valid numeric year.")
    else:
        st.write(f"Number of employees hired for each job and department in {year} divided by quarter. The table must be ordered alphabetically by department and job.")

        query1 = f"""
        SELECT 
            d.department AS department_name,
            j.job AS job_name,
            SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM 
            hired_employees e
        JOIN 
            departments d ON e.department_id = d.id
        JOIN 
            jobs j ON e.job_id = j.id
        WHERE 
            EXTRACT(YEAR FROM e.datetime) = {year}
        GROUP BY 
            d.department, j.job
        ORDER BY 
            d.department ASC, 
            j.job ASC;
        """

        try:
            conn = get_snowflake_connection()
            if conn is not None:
                cursor = conn.cursor()
                cursor.execute(query1)
                results1 = cursor.fetchall()

                # Obtener los nombres de las columnas
                columns1 = [desc[0] for desc in cursor.description]

                # Crear un DataFrame con los resultados
                report_df1 = pd.DataFrame(results1, columns=columns1)

                # Mostrar el DataFrame
                st.dataframe(report_df1)

                st.write(f"List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in {year} for all the departments, ordered by the number of employees hired (descending).")

                query2 = f"""
                WITH department_hires AS (
                    SELECT 
                        d.id AS department_id,
                        d.department AS department_name,
                        COUNT(e.id) AS total_hires
                    FROM 
                        hired_employees e
                    JOIN 
                        departments d ON e.department_id = d.id
                    WHERE 
                        EXTRACT(YEAR FROM e.datetime) = {year}
                    GROUP BY 
                        d.id, d.department
                ),
                average_hires AS (
                    SELECT 
                        AVG(total_hires) AS mean_hires
                    FROM 
                        department_hires
                )
                SELECT 
                    dh.department_id,
                    dh.department_name,
                    dh.total_hires
                FROM 
                    department_hires dh
                CROSS JOIN 
                    average_hires ah
                WHERE 
                    dh.total_hires > ah.mean_hires
                ORDER BY 
                    dh.total_hires DESC;
                """

                cursor.execute(query2)
                results2 = cursor.fetchall()

                # Obtener los nombres de las columnas
                columns2 = [desc[0] for desc in cursor.description]

                # Crear un DataFrame con los resultados
                report_df2 = pd.DataFrame(results2, columns=columns2)

                # Mostrar el DataFrame
                st.dataframe(report_df2)

                cursor.close()
                conn.close()
            else:
                st.error("Failed to connect to Snowflake for the report.")
        except Exception as e:
            st.error(f"Error generating the report: {e}")

