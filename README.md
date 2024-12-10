Globant’s Data Engineering Coding Challenge
Este repositorio contiene dos soluciones para el desafío técnico, una aplicación interactiva con Streamlit y un pipeline automatizado con Snowflake y AWS S3.

Solución 1: Aplicación Interactiva con Streamlit
Una aplicación en Streamlit que permite:
* Cargar datos desde archivos CSV (departments, jobs, hired_employees).
* Validar y cargar datos en una base de datos Snowflake.
* Generar reportes basados en:
  ** Empleados contratados por trimestre en 2021.
  ** Departamentos con contrataciones superiores al promedio en 2021.

Instrucciones
1. Clonar el repositorio: https://github.com/Ajguio/stream_company_data/
2. Instalar dependencias: pip install -r requirements.txt
3. Ejecutar: streamlit run app.py


Solución 2: Pipeline Automatizado con Snowflake y AWS S3
Pipeline escalable que:
* Procesa datos desde un bucket S3 usando stages en Snowflake.
* Separa los datos en esquemas raw (datos crudos) y target (datos procesados).
* Usa tareas programadas y MERGE para cargar nuevos registros evitando duplicados.

Instrucciones
1. Configurar un bucket S3 con los CSV.
2. Configurar Snowflake:
  ** Ejecutar el script snowflake_pipeline.sql para crear tablas, tareas y stages.
3. Activar la tarea programada: ALTER TASK company_data.raw.process_hired_employees RESUME;


Comparación Rápida Soluciones

Aspecto: 
1. Interacción:
    ** Streamlit             --  Interfaz gráfica
    ** Snowflake + AWS S3    --  Automatización sin intervención manual
2. Carga de datos	
    ** Streamlit             --  Manual desde la app
    ** Snowflake + AWS S3    --  Archivos en S3 procesados automáticamente
3. Escalabilidad
    ** Streamlit             --  Limitada
    ** Snowflake + AWS S3    --  Altamente escalable
4. Uso ideal
    ** Streamlit             --  Demo interactiva	
    ** Snowflake + AWS S3    --  Producción y grandes volúmenes de datos

Elección según Caso de Uso

Streamlit: Ideal para pruebas rápidas y demostraciones.
Snowflake + S3: Mejor opción para un entorno de producción.

Autor: Andres J. Guio
Contacto: andresjguio@gmail.com
