import pandas as pd
import psycopg2
import os
import logging

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('carga_datos.log'),
        logging.StreamHandler()
    ]
)

def conectar_db():
    try:
        conexion = psycopg2.connect(
            dbname="Mark III",
            user="test",
            password="test",
            host="localhost",
            port="5432"
        )
        logging.info("Conexión a la base de datos establecida con éxito.")
        return conexion
    except psycopg2.Error as e:
        logging.error(f"Error de conexión a la base de datos: {e}")
        raise

def crear_tabla_proveedor(cursor, nombre_tabla):
    consulta = f'''
        CREATE TABLE IF NOT EXISTS {nombre_tabla} (
            id SERIAL PRIMARY KEY,
            codigo VARCHAR(255) UNIQUE NOT NULL,
            descripcion TEXT,
            marca VARCHAR(255),
            precio NUMERIC
        );
    '''
    try:
        cursor.execute(consulta)
        logging.info(f"Tabla '{nombre_tabla}' creada o ya existente.")
    except psycopg2.Error as e:
        logging.error(f"Error al crear la tabla {nombre_tabla}: {e}")
        raise

def cargar_datos_proveedor(conexion, nombre_tabla, ruta_archivo):
    try:
        with conexion.cursor() as cursor:
            crear_tabla_proveedor(cursor, nombre_tabla)
            
            # Leer datos desde el archivo Excel
            try:
                df = pd.read_excel(ruta_archivo, engine='openpyxl')
                logging.info(f"Archivo '{ruta_archivo}' leído correctamente.")
            except Exception as e:
                logging.error(f"Error al leer el archivo {ruta_archivo}: {e}")
                return

            # Limpiar nombres de columnas
            df.columns = df.columns.str.strip().str.upper()

            logging.info(f'Columnas en el archivo {ruta_archivo}: {df.columns.tolist()}')

            consulta = f'''
                INSERT INTO {nombre_tabla} (codigo, descripcion, marca, precio)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (codigo) DO NOTHING;
            '''

            registros_exitosos = 0
            for index, row in df.iterrows():
                try:
                    cursor.execute(consulta, (row['CODIGO'], row['DESCRIPCION'], row['MARCA'], row['PRECIO']))
                    registros_exitosos += 1
                except psycopg2.Error as e:
                    logging.error(f"Error al insertar datos en la tabla {nombre_tabla} para el registro {index}: {e}")
                    conexion.rollback()  # Realiza rollback en caso de error
                    break  # Detiene la inserción de registros si hay un error

            conexion.commit()
            logging.info(f"Datos cargados exitosamente en la tabla '{nombre_tabla}'. Total de registros insertados: {registros_exitosos}")

    except Exception as e:
        logging.error(f"Error en el proceso de carga para la tabla {nombre_tabla}: {e}")

def cargar_datos_masivos(directorio_archivos):
    conexion = conectar_db()
    try:
        for archivo in os.listdir(directorio_archivos):
            if archivo.endswith('.xlsx'):
                nombre_tabla = archivo.split('.')[0]
                ruta_archivo = os.path.join(directorio_archivos, archivo)
                
                logging.info(f'Cargando datos para la tabla {nombre_tabla} desde {ruta_archivo}...')
                cargar_datos_proveedor(conexion, nombre_tabla, ruta_archivo)
    except Exception as e:
        logging.error(f"Error durante la carga masiva de datos: {e}")
    finally:
        conexion.close()
        logging.info("Conexión a la base de datos cerrada.")

directorio_archivos = 'C:/Users/MOSTRADOR 4/Desktop/Archivo/Tareas wismi/Prototipo Mark III/directorio de descargas'
cargar_datos_masivos(directorio_archivos)