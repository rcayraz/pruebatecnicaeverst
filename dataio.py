import requests
import zipfile
import io
import os
import json
import pandas as pd
import psycopg2
from psycopg2 import sql
import utils as ut

class SMNApi:
    def __init__(self, url):
        self.url = url

    def get_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            return response.content
        else:
            raise Exception(f"Error al obtener datos del API: {response.status_code}")


    def get_dataframe(self):
        data = self.get_data()
        df = self.unzip_data(data)
        return df
    
class JsonData:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.data = None
        self.folder=None
    
    def read_json_files(self):
        all_data = []
        for filename in os.listdir(self.folder_path):
                with open(os.path.join(self.folder_path, filename),encoding='latin-1') as f:
                    data = pd.read_json(f)
                    all_data.append(data)
        self.data = pd.concat(all_data, ignore_index=True)
        df=self.data
        
        return  df
    
    def show_data(self):
        if self.data is not None:
            print(self.data)
        else:
            raise ValueError('No hay datos para mostrar, primero leer los archivos JSON')



class PostgresDB:
    def __init__(self):
        self.dbname = ut.dbname
        self.user = ut.user
        self.password = ut.password
        self.host = ut.host
        self.port = ut.port
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print("Conexión exitosa a PostgreSQL")
        except Exception as e:
            print("Error al conectarse a PostgreSQL: ", e)



class PostgresDB:
    def __init__(self):
        self.dbname = ut.dbname
        self.user = ut.user
        self.password = ut.password
        self.host = ut.host
        self.port = ut.port
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print("Conexión exitosa a PostgreSQL")
        except Exception as e:
            print("Error al conectarse a PostgreSQL: ", e)

    def create_table(self, table_name, df):
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()

        # Crear sentencia SQL para crear la tabla
        create_table_query = f"CREATE TABLE {table_name} ("
        for col in df.columns:
            data_type = df[col].dtype
            if "int" in str(data_type):
                create_table_query += f"{col} INTEGER,"
            elif "float" in str(data_type):
                create_table_query += f"{col} FLOAT,"
            else:
                create_table_query += f"{col} VARCHAR(255),"
        create_table_query = create_table_query[:-1] + ");"

        # Ejecutar sentencia SQL para crear la tabla
        try:
            cursor.execute(create_table_query)
            self.conn.commit()
            print(f"Tabla {table_name} creada exitosamente")
        except Exception as e:
            self.conn.rollback()
            print(f"Error al crear la tabla {table_name}: {e}")
        finally:
            cursor.close()

    def insert_data(self, table_name, df):
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()

        # Crear sentencia SQL para insertar los datos
        insert_query = f"INSERT INTO {table_name} ("
        for col in df.columns:
            insert_query += f"{col},"
        insert_query = insert_query[:-1] + ") VALUES ("
        for _ in df.columns:
            insert_query += "%s,"
        insert_query = insert_query[:-1] + ");"

        # Ejecutar sentencia SQL para insertar los datos
        try:
            for row in df.itertuples(index=False, name=None):
                cursor.execute(insert_query, row)
            self.conn.commit()
            print(f"Datos insertados exitosamente en la tabla {table_name}")
        except Exception as e:
            self.conn.rollback()
            print(f"Error al insertar los datos en la tabla {table_name}: {e}")
        finally:
            cursor.close()
            
    def ejecutar_query(self, query):
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return result        
