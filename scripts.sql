SELECT CURRENT_ACCOUNT();

---> set the Role
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;

--PASO EN AWS: Crear un rol "SnowflakeIntegrationRole " para establecer relacion de confianza entre AWS y snowflake
-- Con el ARN del rol, creo una integracion, asi:

CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::195275668159:role/SnowflakeIntegrationRole'
STORAGE_ALLOWED_LOCATIONS = ('s3://glchallenge/filesemployees/');

-- Crear una nueva base de datos
CREATE OR REPLACE DATABASE company_data;

-- Usar la nueva base de datos
USE DATABASE company_data;

-- Crear nuevos esquemas
CREATE SCHEMA company_data.raw; -- Esquema RAW / DES
CREATE SCHEMA company_data.hiring_data; -- -- Esquema TARGET / PROD

-- Tabla departments
CREATE OR REPLACE TABLE company_data.hiring_data.departments (
    id INTEGER PRIMARY KEY,    -- Primary Key para identificar de forma única el departamento
    department STRING          -- Nombre del departamento
);

-- Tabla jobs
CREATE OR REPLACE TABLE company_data.hiring_data.jobs (
    id INTEGER PRIMARY KEY,    -- Primary Key para identificar de forma única el trabajo
    job STRING                 -- Nombre del trabajo
);

-- Tabla hired_employees
CREATE OR REPLACE TABLE company_data.hiring_data.hired_employees (
    id INTEGER PRIMARY KEY,        -- Primary Key para identificar de forma única al empleado
    name STRING,                   -- Nombre y apellido del empleado
    datetime TIMESTAMP_NTZ,        -- Fecha y hora de contratación
    department_id INTEGER,         -- Foreign Key hacia departments
    job_id INTEGER,                -- Foreign Key hacia jobs
    CONSTRAINT fk_department FOREIGN KEY (department_id) REFERENCES departments(id),
    CONSTRAINT fk_job FOREIGN KEY (job_id) REFERENCES jobs(id)
);

--Tabla de Metadatos // Registra los archivos procesados.
CREATE OR REPLACE TABLE company_data.raw.processed_files (
    file_name STRING PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de Progreso // Crea una tabla que registre el progreso de los archivos procesados, incluyendo el número de registros procesados hasta el momento.
CREATE OR REPLACE TABLE company_data.raw.file_progress (
    file_name STRING PRIMARY KEY,
    records_processed INTEGER DEFAULT 0,
    total_records INTEGER,
    last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--Tabla Temporal // Usada para procesar los datos antes del MERGE.
CREATE OR REPLACE TABLE company_data.raw.temp_hired_employees (
    id INTEGER,
    name STRING,
    datetime TIMESTAMP_NTZ,
    department_id INTEGER,
    job_id INTEGER,
    file_name STRING,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla Temporal para Registros Totales // Calcula y guarda el número de registros totales por archivo desde el STAGE en una tabla.
CREATE OR REPLACE TABLE company_data.raw.stage_file_totals (
    file_name STRING PRIMARY KEY,
    total_records INTEGER
);

-- Creacion de los STAGE en RAW por cada archivo
CREATE OR REPLACE STAGE company_data.raw.stage_departments
STORAGE_INTEGRATION = my_s3_integration
URL = 's3://glchallenge/filesemployees/departments/';

CREATE OR REPLACE STAGE company_data.raw.stage_jobs
STORAGE_INTEGRATION = my_s3_integration
URL = 's3://glchallenge/filesemployees/jobs/';

CREATE OR REPLACE STAGE company_data.raw.stage_hired_employees
STORAGE_INTEGRATION = my_s3_integration
URL = 's3://glchallenge/filesemployees/hired_employees/';

-- Vista para Archivos Incrementales // Esta vista filtra los datos nuevos disponibles en el stage.
CREATE OR REPLACE VIEW company_data.raw.incremental_stage_files AS
SELECT 
    stage.METADATA$FILENAME AS file_name,
    stage.$1 AS id,
    stage.$2 AS name,
    stage.$3 AS datetime,
    stage.$4 AS department_id,
    stage.$5 AS job_id
FROM @company_data.raw.stage_hired_employees stage
WHERE NOT EXISTS (
    -- Excluir registros ya en temp_hired_employees
    SELECT 1
    FROM company_data.raw.temp_hired_employees temp
    WHERE stage.$1 = temp.id 
)
AND NOT EXISTS (
    -- Excluir registros ya en hiring_employees
    SELECT 1
    FROM company_data.hiring_data.hired_employees hired
    WHERE stage.$1 = hired.id 
)AND NOT EXISTS (
    -- Excluir registros completamente procesados según file_progress
    SELECT 1
    FROM company_data.raw.file_progress progress
    WHERE stage.METADATA$FILENAME = progress.file_name
    AND progress.records_processed >= progress.total_records
);

--Store procedure // El procedimiento realiza el MERGE entre la tabla temporal y la tabla final.
CREATE OR REPLACE PROCEDURE company_data.raw.merge_into_hired_employees()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS $$
    var sql_command = `
        MERGE INTO company_data.hiring_data.hired_employees AS target
        USING (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY id, name ORDER BY loaded_at) AS rn
                FROM company_data.raw.temp_hired_employees
            ) AS ranked
            WHERE rn = 1
        ) AS source
        ON target.id = source.id 
        WHEN MATCHED THEN
            UPDATE SET
                target.datetime = source.datetime,
                target.department_id = source.department_id,
                target.job_id = source.job_id
        WHEN NOT MATCHED THEN
            INSERT (id, name, datetime, department_id, job_id)
            VALUES (source.id, source.name, source.datetime, source.department_id, source.job_id);
    `;

    snowflake.execute({sqlText: sql_command});
    return 'Merge completed successfully.';
$$;

--TAREA que se ejecuta cada cierto tiempo con varias actividades 
CREATE OR REPLACE TASK company_data.raw.process_hired_employees
SCHEDULE = '2 MINUTE'
WAREHOUSE = compute_wh
AS
BEGIN
    -- 1. Actualizar totales de registros en stage_file_totals
    MERGE INTO company_data.raw.stage_file_totals AS target
    USING (
        SELECT 
            METADATA$FILENAME AS file_name,
            COUNT(*) AS total_records
        FROM @company_data.raw.stage_hired_employees
        GROUP BY METADATA$FILENAME
    ) AS source
    ON target.file_name = source.file_name
    WHEN MATCHED THEN
        UPDATE SET total_records = source.total_records
    WHEN NOT MATCHED THEN
        INSERT (file_name, total_records)
        VALUES (source.file_name, source.total_records);

    -- 2. Insertar registros desde la vista incremental a la tabla temporal
    INSERT INTO company_data.raw.temp_hired_employees (id, name, datetime, department_id, job_id, file_name)
    SELECT id, name, datetime, department_id, job_id, file_name
    FROM company_data.raw.incremental_stage_files
    order by TO_NUMBER(ID)
    LIMIT 1000;

    -- 3. Ejecutar el procedimiento para realizar el MERGE
    CALL company_data.raw.merge_into_hired_employees();

    -- 4. Actualizar progreso en file_progress
    MERGE INTO company_data.raw.file_progress AS target
    USING (
        SELECT 
            temp.file_name,
            COUNT(*) AS records_processed,
            totals.total_records
        FROM company_data.raw.temp_hired_employees temp
        JOIN company_data.raw.stage_file_totals totals
        ON temp.file_name = totals.file_name
        GROUP BY temp.file_name, totals.total_records
    ) AS source
    ON target.file_name = source.file_name
    WHEN MATCHED THEN
        UPDATE SET
            target.records_processed = source.records_processed,
            target.total_records = source.total_records,
            target.last_processed_at = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (file_name, records_processed, total_records, last_processed_at)
        VALUES (source.file_name, source.records_processed, source.total_records, CURRENT_TIMESTAMP);

    -- 5. Marcar archivos como procesados si se completaron
    INSERT INTO company_data.raw.processed_files (file_name)
    SELECT p.file_name
FROM company_data.raw.file_progress p
JOIN company_data.raw.stage_file_totals t
ON p.file_name = t.file_name
WHERE p.records_processed >= t.total_records
  AND NOT EXISTS (
      SELECT 1
      FROM company_data.raw.processed_files pf
      WHERE pf.file_name = p.file_name
  );

END;

--*********************************VALIDACIONES ***********************************************************
--Activar Tarea
ALTER TASK company_data.raw.process_hired_employees RESUME;
--Desactivar la tarea
ALTER TASK company_data.raw.process_hired_employees SUSPEND;


-- Validacion de los archivos en los buckets
LIST @company_data.raw.stage_departments;
LIST @company_data.raw.stage_jobs;
LIST @company_data.raw.stage_hired_employees;


SELECT * FROM company_data.raw.incremental_stage_files; -- VISTA, que mapea que no se ha procesado

SELECT * FROM company_data.raw.stage_file_totals; -- Total de registros desde stage / Cuando se ejecuta la tarea 

SELECT * FROM company_data.raw.file_progress; -- Progeso de carga de archivos

SELECT * FROM company_data.raw.processed_files; -- Archivos procesados

SELECT * FROM company_data.raw.temp_hired_employees; -- Registros procesados en RAW / DES

SELECT * FROM company_data.hiring_data.hired_employees order by id; -- Registros procesados en TARGET / PROD

--*********************************LIMPIAR REGISTRS ***********************************************************

truncate company_data.raw.stage_file_totals;
truncate company_data.raw.temp_hired_employees;
truncate company_data.hiring_data.hired_employees;
truncate company_data.raw.file_progress;
truncate company_data.raw.processed_files;

--*********************************CARGA MANUAL ***********************************************************

-- Cargar datos en la tabla departments
COPY INTO company_data.hiring_data.departments
FROM @company_data.raw.stage_departments/departments.csv
FILE_FORMAT = (TYPE = 'CSV' /*FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1*/);

-- Cargar datos en la tabla jobs
COPY INTO company_data.hiring_data.jobs
FROM @company_data.raw.stage_jobs/jobs.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' /*SKIP_HEADER = 1*/);

-- Cargar datos en la tabla hired_employees
COPY INTO company_data.hiring_data.hired_employees
--FROM @company_data.raw.stage_hired_employees/ -- OJO! aca cargamos TODO ...
FROM @company_data.raw.stage_hired_employees/hired_employees.csv -- OJO! aca cargamos solo un archivo ...
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"'  /*SKIP_HEADER = 1*/);


select * from departments;
TRUNCATE TABLE departments;

select * from jobs;
TRUNCATE TABLE jobs;

select * from hired_employees order by  ID;
TRUNCATE TABLE hired_employees;

--*********************************CONSULTAS SQL ***********************************************************
-- empleados y sus departamentos
SELECT e.id AS employee_id, e.name, e.datetime, d.department, j.job
FROM company_data.hiring_data.hired_employees e
JOIN company_data.hiring_data.departments d ON e.department_id = d.id
JOIN jobs j ON e.job_id = j.id;

--Número de empleados por departamento:

SELECT d.department, COUNT(e.id) AS total_employees
FROM company_data.hiring_data.hired_employees e
JOIN company_data.hiring_data.departments d ON e.department_id = d.id
GROUP BY d.department
ORDER BY total_employees DESC;

/*Number of employees hired for each job and department in 2021 divided by quarter. The
table must be ordered alphabetically by department and job.*/


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
    EXTRACT(YEAR FROM e.datetime) = 2021
GROUP BY 
    d.department, j.job
ORDER BY 
    d.department ASC, 
    j.job ASC;

/*List of ids, name and number of employees hired of each department that hired more
employees than the mean of employees hired in 2021 for all the departments, ordered
by the number of employees hired (descending).*/
--Common Table Expressions (CTEs)   
WITH department_hires AS (
    -- Número de empleados contratados por cada departamento en 2021
    SELECT 
        d.id AS department_id,
        d.department AS department_name,
        COUNT(e.id) AS total_hires
    FROM 
        hired_employees e
    JOIN 
        departments d ON e.department_id = d.id
    WHERE 
        EXTRACT(YEAR FROM e.datetime) = 2021
    GROUP BY 
        d.id, d.department
),
average_hires AS (
    -- Promedio de contrataciones en 2021
    SELECT 
        AVG(total_hires) AS mean_hires
    FROM 
        department_hires
)
-- Departamentos con más contrataciones que el promedio
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

    
