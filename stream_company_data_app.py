insert_data_to_snowflake(table_name, dataframe):
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
        data = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]

        # **DIAGNÓSTICO**
        st.write("Generated SQL:", sql)
        st.write("Data Prepared for Insertion:", data[:5] if data else "No data")

        # Imprimir para depuración
        print("Generated SQL:", sql)
        print("Data Sample:", data[:5] if data else "No data")

        # Ejecutar la inserción en bloque
        cursor.executemany(sql, data)

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error inserting into {table_name}: {e}")
        print(f"Error inserting into {table_name}: {e}")
        return False

        cursor = conn.cursor()

        # Limpiar datos antes de insertar
        dataframe = clean_dataframe(dataframe, table_name)

        # Generar consulta con marcadores de posición
        placeholders = ', '.join(['?'] * len(dataframe.columns))
        columns = ', '.join(dataframe.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Convertir DataFrame a lista de tuplas
        data = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]

        # **DIAGNÓSTICO**
        st.write("Generated SQL:", sql)
        st.write("First Row of Data:", data[0] if data else "No data")

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
        if file_name not in FILE_TABLE_MAPPING:
            st.error("The file does not match any expected table.")
        else:
            # Leer el archivo sin encabezado
            dataframe = pd.read_csv(uploaded_file, header=None)

            # Determinar la tabla destino
            table_name = FILE_TABLE_MAPPING[file_name]

            # Limpiar el DataFrame
            dataframe = clean_dataframe(dataframe, file_name)

            st.write(f"Inserting data into table: {table_name}")
            success = insert_data_to_snowflake(table_name, dataframe)

            if success:
                st.success(f"Data successfully inserted into {table_name}.")
                st.dataframe(dataframe)
            else:
                st.error(f"Failed to insert data into {table_name}.")
    except Exception as e:
        st.error(f"Error processing the file: {e}")
