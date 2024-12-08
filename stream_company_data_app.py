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

# Cargar los datos desde la URL
try:
    csv_url = "https://glchallenge.s3.us-west-1.amazonaws.com/filesemployees/hired_employees/hired_employees.csv"
    my_fruit_list = pd.read_csv(csv_url)
    st.dataframe(my_fruit_list)  # Mostrar los datos en la interfaz
except Exception as e:
    st.error(f"Error al cargar el archivo CSV: {e}")
