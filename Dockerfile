# Usar Python 3.8 como base
FROM python:3.8-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Copiar los archivos de requisitos primero para aprovechar la caché de Docker
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de los archivos del proyecto
COPY . .

# Crear directorio para datos
RUN mkdir -p data

# Crear directorio para credenciales de Kaggle
RUN mkdir -p /root/.kaggle

# Variables de entorno necesarias
ENV PYTHONUNBUFFERED=1

# Comando por defecto
CMD ["python", "etl_spotify.py"] 