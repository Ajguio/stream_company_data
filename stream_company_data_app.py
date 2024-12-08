import streamlit as st
import pandas as pd

# Título principal
st.title('My Parents New Healthy Diner')

# Encabezado del menú
st.header('Breakfast Menu')

# Elementos del menú
st.text('Omega 3 & Blueberry Oatmeal')
st.text('Kale, Spinach & Rocket Smoothie')
st.text('Hard-Boiled Free-Range Egg')

# Debemos darle permisos al bucket en S3.

# Cabecera que asignaremos manualmente
column_names = ['id', 'name', 'datetime', 'department_id', 'job_id']

# Cargar los datos desde la URL
try:
    csv_url = "https://glchallenge.s3.us-west-1.amazonaws.com/filesemployees/hired_employees/hired_employees.csv"
    
    # Leer el archivo sin encabezado y asignar las columnas manualmente
    my_fruit_list = pd.read_csv(csv_url, header=None, names=column_names)
    
    # Mostrar los datos en la interfaz
    st.dataframe(my_fruit_list)
    
except Exception as e:
    st.error(f"Error al cargar el archivo CSV: {e}")

