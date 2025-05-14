# ETL: Spotify Playlists a MongoDB Atlas

Este proyecto implementa un proceso ETL (Extraction, Transformation, Loading) que extrae datos del dataset de Spotify Playlists desde Kaggle (1.2GB), realiza transformaciones en los datos y los carga en MongoDB Atlas.

## Requisitos

- Python 3.8+
- Cuenta en Kaggle (para usar su API)
- Cuenta en MongoDB Atlas (para la base de datos)
- Pip (gestor de paquetes de Python)

## Instalación

1. Clona este repositorio:
   ```bash
   git clone https://github.com/tu-usuario/etl-spotify-to-mongodb.git
   cd etl-spotify-to-mongodb
   ```

2. Crea un entorno virtual e instala las dependencias:
   ```bash
   python -m venv venv
   
   # En Windows
   venv\Scripts\activate
   
   # En macOS/Linux
   source venv/bin/activate
   
   # Instalar dependencias
   pip install -r requirements.txt
   ```

3. Configura las credenciales:
   - Copia el archivo `.env.example` a `.env`
   - Edita el archivo `.env` con tus credenciales de Kaggle y MongoDB Atlas

## Configuración

### Credenciales de Kaggle

Para usar la API de Kaggle, necesitas:
1. Crear una cuenta en [Kaggle](https://www.kaggle.com)
2. Ir a tu perfil → Account → Create New API Token
3. Esto descargará un archivo `kaggle.json` con tus credenciales
4. Actualiza tu archivo `.env` con estos valores

### MongoDB Atlas

Para usar MongoDB Atlas, necesitas:
1. Crear una cuenta en [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
2. Crear un cluster (puedes usar la opción gratuita)
3. Crear un usuario de base de datos con permisos adecuados
4. Obtener la cadena de conexión (Connection String)
5. Actualiza tu archivo `.env` con esta cadena de conexión

## Uso

Para ejecutar el proceso ETL completo:

```bash
python etl_spotify.py
```

El proceso realizará las siguientes acciones:
1. **Extracción**: Descarga el dataset "Spotify Playlists" desde Kaggle (1.2GB)
2. **Transformación**: Limpia y transforma los datos (maneja valores nulos, duplicados, outliers, etc.)
3. **Carga**: Inserta los datos en MongoDB Atlas

## Estructura del proyecto

```
etl-spotify-to-mongodb/
├── etl_spotify.py     # Script principal de ETL
├── .env               # Variables de entorno (credenciales)
├── .env.example       # Ejemplo de archivo de variables de entorno
├── requirements.txt   # Dependencias del proyecto
├── data/              # Carpeta donde se descargan los datos (creada automáticamente)
└── README.md          # Este archivo
```

## Dependencias

- pandas: Para procesamiento de datos
- numpy: Para operaciones numéricas
- pymongo: Para conectar con MongoDB
- kaggle: API oficial de Kaggle
- python-dotenv: Para cargar variables de entorno
- tqdm: Para barras de progreso
- logging: Para registro de eventos

## Monitorización

El proceso genera logs detallados que se guardan en:
- Archivo: `etl_spotify.log`
- Consola: Muestra información en tiempo real

## Estructura de la base de datos

El ETL crea las siguientes colecciones en MongoDB:

1. **playlists**: Contiene información sobre las listas de reproducción
2. **tracks**: Contiene información detallada sobre las canciones

## Solución de problemas

- **Error de autenticación de Kaggle**: Verifica tus credenciales en el archivo `.env`
- **Error de conexión a MongoDB**: Asegúrate de que la cadena de conexión sea correcta y que tu IP esté en la lista blanca
- **Problemas de memoria**: El script procesa los datos en lotes para minimizar el uso de memoria, pero puedes ajustar el tamaño de los lotes según tu hardware

## Licencia

Este proyecto está licenciado bajo MIT License.