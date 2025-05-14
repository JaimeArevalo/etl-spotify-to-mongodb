"""
Script para analizar el archivo CSV de Spotify
---------------------------------------------
Este script examina el archivo CSV descargado para identificar problemas y mostrar su estructura.
"""

import os
import pandas as pd

def analizar_csv(file_path):
    print(f"Analizando archivo: {file_path}")
    
    # Verificar si el archivo existe
    if not os.path.exists(file_path):
        print(f"El archivo {file_path} no existe.")
        return
    
    # Obtener información básica del archivo
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"Tamaño del archivo: {file_size_mb:.2f} MB")
    
    try:
        # Intentar leer las primeras filas para entender la estructura
        print("\n--- Leyendo primeras 5 filas ---")
        df_head = pd.read_csv(file_path, nrows=5)
        print("Columnas detectadas:")
        for i, col in enumerate(df_head.columns):
            print(f"  {i+1}. {col}")
        print("\nMuestra de datos:")
        print(df_head.head())
        
        # Intentar identificar el delimitador
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            commas = first_line.count(',')
            semicolons = first_line.count(';')
            tabs = first_line.count('\t')
            pipes = first_line.count('|')
        
        print(f"\nPosibles delimitadores en la primera línea:")
        print(f"  Comas (,): {commas}")
        print(f"  Punto y coma (;): {semicolons}")
        print(f"  Tabuladores (\\t): {tabs}")
        print(f"  Pipes (|): {pipes}")
        
        # Verificar si hay comillas que puedan causar problemas de parsing
        with open(file_path, 'r', encoding='utf-8') as f:
            first_few_lines = [f.readline() for _ in range(10)]
            has_quotes = any('"' in line for line in first_few_lines)
        
        print(f"\n¿Contiene comillas dobles (\") en las primeras líneas?: {'Sí' if has_quotes else 'No'}")
        
        # Intentar leer líneas específicas que pueden ser problemáticas
        print("\n--- Intentando leer la línea problemática (14735) ---")
        try:
            # Leer solo esa línea específica (y las cercanas)
            skip_to = 14734  # Línea justo antes de la problemática
            problematic_lines = pd.read_csv(file_path, skiprows=range(1, skip_to), nrows=3, header=None)
            print("Líneas cercanas a la problemática:")
            print(problematic_lines)
        except Exception as e:
            print(f"No se pudo leer la línea problemática específica: {str(e)}")
            
            # Intentar leer como texto simple
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if len(lines) >= 14735:
                        print(f"Línea 14734 (antes de la problemática): {lines[14733].strip()}")
                        print(f"Línea 14735 (problemática): {lines[14734].strip()}")
                        print(f"Línea 14736 (después de la problemática): {lines[14735].strip() if len(lines) > 14735 else 'No hay más líneas'}")
            except Exception as e:
                print(f"Error al leer archivo como texto: {str(e)}")
        
        # Probar lectura con diferentes configuraciones
        print("\n--- Probando diferentes configuraciones de lectura ---")
        
        # 1. Con engine='python'
        try:
            print("Lectura con engine='python'...")
            df_python = pd.read_csv(file_path, nrows=20, engine='python')
            print(f"Éxito! Se leyeron {len(df_python)} filas")
        except Exception as e:
            print(f"Error: {str(e)}")
            
        # 2. Con error_bad_lines=False o on_bad_lines='skip'
        try:
            print("\nLectura con on_bad_lines='skip'...")
            df_skip_bad = pd.read_csv(file_path, nrows=20, on_bad_lines='skip')
            print(f"Éxito! Se leyeron {len(df_skip_bad)} filas")
        except Exception as e:
            try:
                print("Intentando con error_bad_lines=False (para pandas < 1.3)...")
                df_skip_bad = pd.read_csv(file_path, nrows=20, error_bad_lines=False)
                print(f"Éxito! Se leyeron {len(df_skip_bad)} filas")
            except Exception as e2:
                print(f"Error: {str(e2)}")
        
        print("\n--- Análisis completado ---")
    except Exception as e:
        print(f"Error general al analizar el archivo: {str(e)}")

if __name__ == "__main__":
    # Directorio donde se descargó el dataset
    data_dir = "./data"
    
    # Buscar archivos CSV en el directorio
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"No se encontraron archivos CSV en {data_dir}")
    else:
        for csv_file in csv_files:
            csv_path = os.path.join(data_dir, csv_file)
            analizar_csv(csv_path)