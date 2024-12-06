from fastapi import FastAPI, UploadFile, HTTPException
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import MetaData, Table, Column, Integer, String, Date, insert, DateTime, func
from typing import List
from csv import DictReader
import asyncio
from datetime import datetime
from sqlalchemy.sql import text
from operator import itemgetter
# from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# FastAPI app configuration
app = FastAPI()

# connection to azure blob storage
# connection_string = "tu_cadena_de_conexion_aqui"
# container_name = "csv-files"

# Azure SQL connection string
# DATABASE_URL = "mssql+pyodbc://<username>:<password>@<server>/<database>?driver=ODBC+Driver+17+for+SQL+Server"
# async_engine = create_async_engine(DATABASE_URL, echo=True, future=True)

# for testing purposes:
# SQLite database configuration (local file-based DB)
DATABASE_URL = "sqlite+aiosqlite:///./test.db"
async_engine = create_async_engine(DATABASE_URL, echo=True, future=True)

# for testing purposes:
# Define table schemas
metadata = MetaData()

departments_table = Table(
    "departments", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False)
)

jobs_table = Table(
    "jobs", metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String, nullable=False)
)

# Nueva tabla con columna hire_date como DATETIME
employees_table = Table(
    "employees", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("hire_date", DateTime, nullable=False),  # Cambiado de DATE a DATETIME
    Column("department_id", Integer, nullable=False),
    Column("job_id", Integer, nullable=False)
)

# Define schemas for validation
schemas = {
    "departments": ["id", "name"],
    "jobs": ["id", "title"],
    "employees": ["id", "name", "hire_date", "department_id", "job_id"]
}

# for testing purposes:
# Initialize the database
async def init_db():
    async with async_engine.begin() as connection:
        await connection.run_sync(metadata.create_all)

# for testing purposes:
@app.on_event("startup")
async def startup_event():
    await init_db()

# Function to process CSV files
async def process_csv(file: UploadFile, table_name: str):
    try:
        schema = schemas.get(table_name)
        if not schema:
            raise ValueError("Invalid table name")

        # Read the file content
        content = await file.read()
        rows = list(DictReader(content.decode("utf-8").splitlines(), fieldnames=schema))

        # Insert data into the database
        async with async_engine.begin() as connection:
            for row in rows:
                # El campo hire_date solo se procesa si estamos trabajando con la tabla employees
                if table_name == "employees":
                    hire_date = datetime.strptime(row["hire_date"], "%Y-%m-%dT%H:%M:%SZ") if row["hire_date"] else None
                else:
                    hire_date = None  # No procesar hire_date si no es necesario

                # Insertar en la tabla employees
                if table_name == "employees":
                    stmt = insert(employees_table).values(
                        id=row["id"],
                        name=row["name"],
                        hire_date=hire_date,  # Solo se pasa hire_date si está en el archivo
                        department_id=row["department_id"],
                        job_id=row["job_id"]
                    )
                # Insertar en la tabla departments
                elif table_name == "departments":
                    stmt = insert(departments_table).values(
                        id=row["id"],
                        name=row["name"]
                    )
                # Insertar en la tabla jobs
                elif table_name == "jobs":
                    stmt = insert(jobs_table).values(
                        id=row["id"],
                        title=row["title"]
                    )

                # Ejecutar la consulta
                await connection.execute(stmt)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")



# testing endpoint:
@app.get("/")
async def root():
    return {"message": "API is working!"}

# Endpoint to upload CSV files
@app.post("/upload/{table_name}")
async def upload_csv(table_name: str, file: UploadFile):
    if table_name not in schemas.keys():
        raise HTTPException(status_code=400, detail="Invalid table name")

    await process_csv(file, table_name)
    return {"message": f"Data successfully uploaded to the {table_name} table."}

# Endpoint to insert batch data
@app.post("/batch-insert/{table_name}")
async def batch_insert(table_name: str, rows: List[dict]):
    if table_name not in schemas.keys():
        raise HTTPException(status_code=400, detail="Invalid table name")

    if not (1 <= len(rows) <= 1000):
        raise HTTPException(status_code=400, detail="Batch size must be between 1 and 1000")

    try:
        async with async_engine.begin() as connection:
            # Dependiendo de la tabla, usamos la instrucción insert() de SQLAlchemy
            for row in rows:
                if table_name == "employees":
                    stmt = insert(employees_table).values(
                        id=row["id"],
                        name=row["name"],
                        hire_date=row["hire_date"],  # Asegúrate de que `hire_date` sea un datetime válido
                        department_id=row["department_id"],
                        job_id=row["job_id"]
                    )
                elif table_name == "departments":
                    stmt = insert(departments_table).values(
                        id=row["id"],
                        name=row["name"]
                    )
                elif table_name == "jobs":
                    stmt = insert(jobs_table).values(
                        id=row["id"],
                        title=row["title"]
                    )
                
                # Ejecutar la declaración preparada
                await connection.execute(stmt)
                
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing batch: {str(e)}")

    return {"message": f"Batch successfully inserted into the {table_name} table."}


# Endpoint to get the employees hired for each job and department
@app.get("/metrics/employee-hire-quarters")
async def employee_hire_by_quarter():
    try:
        # Definir la consulta como un objeto text()
        query = text("""
            SELECT d.name AS department, j.title AS job, 
                   COUNT(CASE WHEN strftime('%m', e.hire_date) BETWEEN '01' AND '03' THEN 1 END) AS Q1,
                   COUNT(CASE WHEN strftime('%m', e.hire_date) BETWEEN '04' AND '06' THEN 1 END) AS Q2,
                   COUNT(CASE WHEN strftime('%m', e.hire_date) BETWEEN '07' AND '09' THEN 1 END) AS Q3,
                   COUNT(CASE WHEN strftime('%m', e.hire_date) BETWEEN '10' AND '12' THEN 1 END) AS Q4
            FROM employees e
            JOIN departments d ON e.department_id = d.id
            JOIN jobs j ON e.job_id = j.id
            WHERE strftime('%Y', e.hire_date) = '2021'
            GROUP BY d.name, j.title
            ORDER BY d.name, j.title;
        """)

        # Ejecutar la consulta
        async with async_engine.begin() as connection:
            result = await connection.execute(query)
            rows = result.fetchall()

        # Convertir los resultados en formato adecuado para la respuesta
        metrics = [
            {"department": row[0], "job": row[1], "Q1": row[2], "Q2": row[3], "Q3": row[4], "Q4": row[5]}
            for row in rows
        ]

        return metrics
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving data: {str(e)}")

from sqlalchemy.sql import text

# Endpoint departments hired above average
from sqlalchemy.sql import text

@app.get("/metrics/departments-hired-above-average")
async def departments_hired_above_average():
    try:
        # count hired employees on 2021
        query = text("""
            SELECT d.id AS department_id, d.name AS department, 
                   COUNT(e.id) AS hired
            FROM employees e
            JOIN departments d ON e.department_id = d.id
            WHERE strftime('%Y', e.hire_date) = '2021'
            GROUP BY d.id
        """)

        # Ejecutar la consulta
        async with async_engine.begin() as connection:
            result = await connection.execute(query)  # Devuelve un objeto Result
            department_hired_data = result.fetchall()  # Recopila todos los datos en una lista

        # Calcular el promedio de empleados contratados por departamento
        total_employees_hired = sum(row[2] for row in department_hired_data)
        average_hired = total_employees_hired / len(department_hired_data)

        # Filtrar departamentos que contrataron más que el promedio
        above_average_departments = [
            {"id": dept_id, "department": name, "hired": hired}
            for dept_id, name, hired in department_hired_data
            if hired > average_hired
        ]

        # Ordenar por número de empleados contratados de forma descendente
        above_average_departments.sort(key=itemgetter('hired'), reverse=True)

        return above_average_departments
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving data: {str(e)}")
