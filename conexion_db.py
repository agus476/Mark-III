import psycopg2
import logging

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
        
        # Prueba simple para verificar la conexión
        cursor = conexion.cursor()
        cursor.execute("SELECT 1")
        resultado = cursor.fetchone()
        if resultado:
            print("Conexión a la base de datos verificada correctamente.")
        cursor.close()
        
        return conexion
    except psycopg2.Error as e:
        logging.error(f"Error de conexión a la base de datos: {e}")
        raise

# Ejemplo de uso
if __name__ == "__main__":
    conexion = conectar_db()
