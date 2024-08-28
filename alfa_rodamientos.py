import pandas as pd
import psycopg2
import os
import json
import logging
from conexion_db import conectar_db

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('carga_datos.log'),
        logging.StreamHandler()
    ]
)

# Función para cargar la configuración desde un archivo JSON
def cargar_configuracion(ruta_config):
    with open(ruta_config, 'r') as archivo:
        configuracion = json.load(archivo)
    return configuracion

# Función para formatear y extraer datos del archivo según la configuración
def formatear_archivo(ruta_archivo, configuracion):
    header = configuracion['header']
    columns = configuracion['columns']
    column_names = configuracion['column_names']

    # Imprimir la ruta del archivo para verificar
    print(f"Intentando leer el archivo desde: {ruta_archivo}")

    # Detectar la extensión del archivo y usar el método adecuado
    extension = os.path.splitext(ruta_archivo)[1]
    if extension == '.xls':
        df = pd.read_excel(ruta_archivo, header=header, engine='xlrd')
    elif extension == '.xlsx':
        df = pd.read_excel(ruta_archivo, header=header, engine='openpyxl')
    elif extension == '.csv':
        df = pd.read_csv(ruta_archivo, header=header)
    else:
        raise ValueError(f"Formato de archivo no soportado: {extension}")

    df = df.iloc[:, columns]
    df.columns = column_names
    df['precio'] = pd.to_numeric(df['precio'], errors='coerce')
    df.dropna(subset=['codigo', 'precio'], inplace=True)
    return df

# Función para actualizar los precios en la base de datos y registrar los cambios
def actualizar_precios(conexion, nombre_proveedor, df_formateado):
    cursor = conexion.cursor()
    tabla = nombre_proveedor.lower().replace(".", "").replace(" ", "_")

    consulta_precio_antiguo = f'''
        SELECT precio FROM {tabla}
        WHERE codigo = %s;
    '''
    consulta = f'''
        UPDATE {tabla}
        SET precio = %s
        WHERE codigo = %s;
    '''

    for index, row in df_formateado.iterrows():
        try:
            # Convertir el código a cadena para asegurarse de que coincida con el tipo VARCHAR en la base de datos
            codigo = str(row['codigo']).strip()  # Asegurarse de que no haya espacios en blanco
            if not codigo:  # Si el código es vacío, omitir la fila
                continue

            cursor.execute(consulta_precio_antiguo, (codigo,))
            resultado = cursor.fetchone()
            if resultado:
                precio_antiguo = float(resultado[0])
                precio_nuevo = float(row['precio'])
                if precio_antiguo != precio_nuevo:
                    cursor.execute(consulta, (precio_nuevo, codigo))
                    logging.info(f"Código: {codigo} - Precio antiguo: {precio_antiguo} - Nuevo precio: {precio_nuevo}")
        except psycopg2.Error as e:
            logging.error(f"Error al actualizar código {codigo}: {e}")
            conexion.rollback()  # Revertir transacción en caso de error para continuar con la siguiente fila

    conexion.commit()
    cursor.close()

# Función general para procesar el archivo de un proveedor y actualizar precios
def procesar_y_actualizar_precios(directorio_archivos, nombre_proveedor, ruta_config):
    configuracion = cargar_configuracion(ruta_config)
    if nombre_proveedor not in configuracion:
        raise ValueError(f"Proveedor no reconocido: {nombre_proveedor}")

    conexion = conectar_db()
    if conexion is None:
        return

    nombre_archivo = f'{nombre_proveedor}.xls'  # o .xlsx o .csv según el caso
    ruta_archivo = os.path.join(directorio_archivos, nombre_archivo)

    print(f'Formateando datos para {nombre_proveedor} desde {ruta_archivo}...')
    df_formateado = formatear_archivo(ruta_archivo, configuracion[nombre_proveedor])

    print(f'Actualizando precios para {nombre_proveedor}...')
    actualizar_precios(conexion, nombre_proveedor, df_formateado)

    conexion.close()

# Ruta del directorio que contiene el archivo Excel o CSV
directorio_archivos = 'C:/Users/MOSTRADOR 4/Desktop/Archivo/Tareas wismi/Prototipo Mark III/directorio de descargas'
ruta_config = 'C:/Users/MOSTRADOR 4/Desktop/Archivo/Tareas wismi/Prototipo Mark III/config.json'
nombre_proveedor = 'alfa_rodamientos'

# Procesar el archivo y actualizar precios
procesar_y_actualizar_precios(directorio_archivos, nombre_proveedor, ruta_config)
