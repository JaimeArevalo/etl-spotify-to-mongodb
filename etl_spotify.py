"""
ETL: Spotify Playlists a MongoDB Atlas
-------------------------------------
Este script realiza un proceso ETL (Extracción, Transformación, Carga) para:
1. Extraer datos del dataset de Spotify Playlists de Kaggle (1.2GB)
2. Transformar y limpiar los datos
3. Cargar los datos en MongoDB Atlas
"""

import os
import sys
import json
import time
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import logging
from tqdm import tqdm

# Cargar variables de entorno (necesarias para las credenciales)
load_dotenv(verbose=True)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_spotify.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("spotify-etl")

def configurar_kaggle_api():
    """Configura la API de Kaggle usando las credenciales del sistema"""
    try:
        # Verificar si las credenciales de Kaggle están disponibles
        kaggle_dir = os.path.expanduser('~/.kaggle')
        kaggle_json = os.path.join(kaggle_dir, 'kaggle.json')
        
        if not os.path.exists(kaggle_json):
            # Crear el directorio si no existe
            os.makedirs(kaggle_dir, exist_ok=True)
            
            # Crear el archivo de credenciales a partir de variables de entorno
            creds = {
                "username": os.environ.get("KAGGLE_USERNAME"),
                "key": os.environ.get("KAGGLE_KEY")
            }
            
            # Verificar que las credenciales existan
            if not creds["username"] or not creds["key"]:
                raise ValueError("KAGGLE_USERNAME o KAGGLE_KEY no están definidas en las variables de entorno")
            
            logger.info(f"Creando archivo de credenciales en {kaggle_json}")
            with open(kaggle_json, 'w') as f:
                json.dump(creds, f)
            
            # Cambiar permisos para que solo el propietario pueda leer/escribir
            os.chmod(kaggle_json, 0o600)
            
            logger.info(f"Archivo de credenciales creado en {kaggle_json}")
        
        api = KaggleApi()
        api.authenticate()
        logger.info("✅ API de Kaggle autenticada correctamente")
        return api
    except Exception as e:
        logger.error(f"❌ Error al configurar la API de Kaggle: {str(e)}")
        raise

def descargar_dataset(api, dataset_name="andrewmvd/spotify-playlists", path="./data"):
    """Descarga el dataset de Spotify Playlists de Kaggle"""
    try:
        # Crear el directorio si no existe
        os.makedirs(path, exist_ok=True)
        
        logger.info(f"🔄 Descargando el dataset {dataset_name}...")
        api.dataset_download_files(dataset_name, path=path, unzip=True)
        logger.info(f"✅ Dataset descargado y descomprimido en {path}")
        
        # Listar archivos descargados
        files = os.listdir(path)
        logger.info(f"📁 Archivos descargados: {files}")
        
        return files
    except Exception as e:
        logger.error(f"❌ Error al descargar el dataset: {str(e)}")
        raise

def conectar_mongodb():
    """Establece conexión con MongoDB Atlas"""
    try:
        # Obtener connection string de las variables de entorno
        connection_string = os.environ.get("MONGODB_CONNECTION_STRING")
        
        if not connection_string:
            raise ValueError("MONGODB_CONNECTION_STRING no está definida en las variables de entorno")
        
        # Conectar a MongoDB Atlas
        client = MongoClient(connection_string)
        
        # Verificar conexión
        client.admin.command('ping')
        logger.info("✅ Conexión a MongoDB Atlas establecida correctamente")
        
        # Crear y retornar la base de datos - usando spotify_music_db como nombre de la base de datos
        db = client["spotify_music_db"]
        return db
    except Exception as e:
        logger.error(f"❌ Error al conectar con MongoDB Atlas: {str(e)}")
        raise

def limpiar_y_transformar_playlists(df_playlists):
    """Limpia y transforma el dataframe de playlists"""
    logger.info("🔄 Limpiando y transformando datos de playlists...")
    
    # Copia para no modificar el original
    df = df_playlists.copy()
    
    # Eliminar duplicados
    num_duplicados = df.duplicated().sum()
    df.drop_duplicates(inplace=True)
    logger.info(f"🧹 Se eliminaron {num_duplicados} registros duplicados")
    
    # Manejar valores nulos en campos importantes
    if 'name' in df.columns:
        df['name'].fillna('Unknown Playlist', inplace=True)
    
    # Convertir columnas de fecha a datetime si existen
    date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        except:
            pass
    
    # Limpiar caracteres especiales en campos de texto
    text_columns = df.select_dtypes(include=['object']).columns
    for col in text_columns:
        df[col] = df[col].astype(str).str.replace(r'[^\w\s]', '', regex=True)
    
    logger.info(f"✅ Transformación completada. Dimensiones del dataset: {df.shape}")
    return df

def limpiar_y_transformar_tracks(df_tracks):
    """Limpia y transforma el dataframe de tracks"""
    logger.info("🔄 Limpiando y transformando datos de tracks...")
    
    # Copia para no modificar el original
    df = df_tracks.copy()
    
    # Eliminar duplicados
    num_duplicados = df.duplicated().sum()
    df.drop_duplicates(inplace=True)
    logger.info(f"🧹 Se eliminaron {num_duplicados} registros duplicados")
    
    # Manejar valores nulos en campos importantes
    if 'track_name' in df.columns:
        df['track_name'].fillna('Unknown Track', inplace=True)
    
    if 'artist_name' in df.columns:
        df['artist_name'].fillna('Unknown Artist', inplace=True)
    
    # Normalizar valores numéricos (como popularidad, duración, etc.)
    numeric_columns = df.select_dtypes(include=['number']).columns
    for col in numeric_columns:
        # Reemplazar valores extremos (outliers) con NaN y luego con la mediana
        q1 = df[col].quantile(0.05)
        q3 = df[col].quantile(0.95)
        iqr = q3 - q1
        lower_bound = q1 - (1.5 * iqr)
        upper_bound = q3 + (1.5 * iqr)
        
        # Reemplazar outliers con NaN
        df.loc[(df[col] < lower_bound) | (df[col] > upper_bound), col] = np.nan
        
        # Reemplazar NaN con la mediana
        df[col].fillna(df[col].median(), inplace=True)
    
    # Crear categorías de popularidad si existe columna de popularidad
    if 'popularity' in df.columns:
        df['popularity_category'] = pd.cut(
            df['popularity'], 
            bins=[0, 20, 40, 60, 80, 100],
            labels=['Very Low', 'Low', 'Medium', 'High', 'Very High']
        )
    
    logger.info(f"✅ Transformación completada. Dimensiones del dataset: {df.shape}")
    return df

def cargar_en_mongodb(db, dataframe, collection_name, batch_size=1000):
    """Carga un dataframe en MongoDB Atlas en lotes"""
    try:
        collection = db[collection_name]
        
        # Convertir el dataframe a una lista de diccionarios
        records = json.loads(dataframe.to_json(orient='records', date_format='iso'))
        
        total_records = len(records)
        logger.info(f"🔄 Cargando {total_records} registros en la colección '{collection_name}'...")
        
        # Procesar en lotes para evitar problemas de memoria
        num_batches = (total_records + batch_size - 1) // batch_size  # Redondear hacia arriba
        
        for i in tqdm(range(num_batches), desc=f"Cargando {collection_name}"):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_records)
            batch = records[start_idx:end_idx]
            
            if batch:
                try:
                    collection.insert_many(batch, ordered=False)
                except BulkWriteError as bwe:
                    # Algunos documentos pueden fallar, pero continuamos con el resto
                    logger.warning(f"⚠️ Algunos documentos no se pudieron insertar: {bwe.details['writeErrors']}")
                    
        # Crear índices para optimizar consultas
        if collection_name == 'playlists':
            collection.create_index("id", unique=True)
            collection.create_index("name")
        elif collection_name == 'tracks':
            collection.create_index("id", unique=True)
            collection.create_index("artist_name")
            collection.create_index("track_name")
        
        logger.info(f"✅ Datos cargados correctamente en la colección '{collection_name}'")
        return True
    except Exception as e:
        logger.error(f"❌ Error al cargar datos en MongoDB: {str(e)}")
        raise

def procesar_archivo_csv(file_path, db, collection_name, funcion_transformacion):
    """Procesa un archivo CSV en chunks para manejar archivos grandes"""
    logger.info(f"🔄 Procesando archivo: {file_path}")
    
    # Tamaño de cada chunk
    chunk_size = 100000
    
    try:
        # Intentar leer las primeras filas para entender la estructura del CSV
        logger.info("Analizando la estructura del CSV...")
        sample_data = pd.read_csv(file_path, nrows=5)
        logger.info(f"Estructura detectada: {list(sample_data.columns)}")
        num_columnas = len(sample_data.columns)
        logger.info(f"Número de columnas detectadas: {num_columnas}")
        
        # Usar iterador de chunks para procesar el archivo por partes
        # Con error_bad_lines=False (en pandas < 1.3) o on_bad_lines='skip' (en pandas >= 1.3)
        # para saltar líneas problemáticas
        try:
            # Para versiones nuevas de pandas (>= 1.3)
            chunks = pd.read_csv(file_path, chunksize=chunk_size, on_bad_lines='skip')
            logger.info("Usando 'on_bad_lines=skip' para manejar líneas problemáticas")
        except TypeError:
            # Para versiones antiguas de pandas (< 1.3)
            chunks = pd.read_csv(file_path, chunksize=chunk_size, error_bad_lines=False, warn_bad_lines=True)
            logger.info("Usando 'error_bad_lines=False' para manejar líneas problemáticas")
        
        # Contador para el número total de filas procesadas
        total_filas_procesadas = 0
        
        for i, chunk in enumerate(chunks):
            logger.info(f"🔄 Procesando chunk {i+1}...")
            
            # Contar filas en este chunk
            filas_en_chunk = len(chunk)
            total_filas_procesadas += filas_en_chunk
            
            # Aplicar transformaciones al chunk
            chunk_transformado = funcion_transformacion(chunk)
            
            # Cargar chunk en MongoDB
            cargar_en_mongodb(db, chunk_transformado, collection_name, batch_size=1000)
            
            # Liberar memoria
            del chunk_transformado
            
            logger.info(f"✅ Chunk {i+1} procesado ({filas_en_chunk} filas)")
            
        logger.info(f"✅ Archivo {file_path} procesado completamente. Total de filas: {total_filas_procesadas}")
        return True
    except Exception as e:
        logger.error(f"❌ Error al procesar el archivo {file_path}: {str(e)}")
        
        # Intentar un enfoque más robusto si el primer método falla
        logger.info("Intentando método alternativo de lectura...")
        try:
            # Leer con un parser más flexible (motor C desactivado)
            df = pd.read_csv(file_path, engine='python', encoding='utf-8', encoding_errors='ignore')
            logger.info(f"Lectura alternativa exitosa. Dimensiones: {df.shape}")
            
            # Aplicar transformaciones
            df_transformado = funcion_transformacion(df)
            
            # Cargar en MongoDB
            cargar_en_mongodb(db, df_transformado, collection_name, batch_size=1000)
            
            logger.info(f"✅ Archivo {file_path} procesado completamente con método alternativo")
            return True
        except Exception as e2:
            logger.error(f"❌ Ambos métodos de lectura fallaron. Error final: {str(e2)}")
            raise

def ejecutar_etl():
    """Función principal que ejecuta todo el proceso ETL"""
    inicio = time.time()
    logger.info("🚀 Iniciando proceso ETL de Spotify Playlists a MongoDB Atlas")
    
    try:
        # 0. Imprimir el directorio actual (para depuración)
        logger.info(f"Directorio actual: {os.getcwd()}")
        
        # 0.1 Cargar variables de entorno
        load_dotenv()
        
        # 0.2 Verificar que las variables de entorno estén cargadas
        logger.info(f"KAGGLE_USERNAME: {'configurado' if os.environ.get('KAGGLE_USERNAME') else 'no configurado'}")
        logger.info(f"KAGGLE_KEY: {'configurado' if os.environ.get('KAGGLE_KEY') else 'no configurado'}")
        logger.info(f"MONGODB_CONNECTION_STRING: {'configurado' if os.environ.get('MONGODB_CONNECTION_STRING') else 'no configurado'}")
        
        # 1. Configurar API de Kaggle
        api = configurar_kaggle_api()
        
        # 2. Descargar dataset
        archivos = descargar_dataset(api)
        
        # 3. Conectar a MongoDB Atlas
        db = conectar_mongodb()
        
        # 4. Procesar cada archivo y cargarlo en MongoDB
        for archivo in archivos:
            ruta_completa = os.path.join("./data", archivo)
            
            # Determinar qué tipo de datos contiene y aplicar la transformación adecuada
            if "playlist" in archivo.lower():
                procesar_archivo_csv(
                    ruta_completa, 
                    db, 
                    "playlists", 
                    limpiar_y_transformar_playlists
                )
            elif "track" in archivo.lower():
                procesar_archivo_csv(
                    ruta_completa, 
                    db, 
                    "tracks", 
                    limpiar_y_transformar_tracks
                )
            else:
                # Para otros archivos, usar una transformación genérica
                logger.info(f"Archivo no reconocido: {archivo}. Se aplicará transformación genérica.")
                procesar_archivo_csv(
                    ruta_completa, 
                    db, 
                    archivo.split('.')[0], 
                    lambda df: df.dropna(how='all')
                )
        
        fin = time.time()
        tiempo_total = (fin - inicio) / 60  # en minutos
        
        logger.info(f"✅ Proceso ETL completado exitosamente en {tiempo_total:.2f} minutos")
        
        # Mostrar un resumen de las colecciones creadas
        colecciones = db.list_collection_names()
        for col in colecciones:
            count = db[col].count_documents({})
            logger.info(f"📊 Colección '{col}': {count} documentos")
        
        return True
    except Exception as e:
        logger.error(f"❌ Error en el proceso ETL: {str(e)}")
        return False

if __name__ == "__main__":
    # Ejecutar el proceso ETL
    exito = ejecutar_etl()
    
    if exito:
        print("🎉 Proceso ETL finalizado correctamente!")
    else:
        print("❌ El proceso ETL falló. Revisar los logs para más detalles.")