import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from io import BytesIO
import os
import sys
BUCKET_NAME = "etl-aws-sanmiguel"
REGION = "us-east-2"
DOWNLOADS_PATH = Path.home() / "Downloads" / "alumnos.csv"
#  Busca alumnos.csv en la carpeta Downloads
def buscar_archivo_csv():
    print(f"Buscando archivo en: {DOWNLOADS_PATH}")    
    if not DOWNLOADS_PATH.exists():
        print(f"ERROR: No se encontró {DOWNLOADS_PATH}")
        print(f"   Verifica que el archivo esté en Downloads")
        return None
    print(f"Archivo encontrado: {DOWNLOADS_PATH}")
    size_kb = DOWNLOADS_PATH.stat().st_size / 1024
    print(f"   • Tamaño: {size_kb:.2f} KB")
    
    return DOWNLOADS_PATH
#Convierte CSV a Parquet en memoria
def convertir_csv_a_parquet(csv_path):  
    try:
        df = pd.read_csv(csv_path)
        print(f"CSV cargado")
        print(f"Registros: {len(df):,}")
        print(f"Columnas: {', '.join(df.columns.tolist())}")
        df = df.convert_dtypes()
        print(f"\nPrimeras 3 filas:")
        print(df.head(3).to_string(index=False))
        # Convertir a Parquet en memoria
        print(f"\nConvirtiendo a Parquet...")
        parquet_buffer = BytesIO()   
        df.to_parquet(
            parquet_buffer,
            engine='pyarrow',
            compression='snappy',
            index=False
        )      
        parquet_buffer.seek(0)       
        csv_size = csv_path.stat().st_size / 1024
        parquet_size = len(parquet_buffer.getvalue()) / 1024
        compression = (1 - parquet_size / csv_size) * 100      
        print(f"Conversión completada")
        print(f"   • Tamaño CSV: {csv_size:.2f} KB")
        print(f"   • Tamaño Parquet: {parquet_size:.2f} KB")
        print(f"   • Compresión: {compression:.1f}%")
        
        return parquet_buffer, len(df)
        
    except Exception as e:
        print(f"ERROR en conversión: {str(e)}")
        return None, 0
# Sube el Parquet a S3 con partición por fecha
def subir_a_s3_particionado(parquet_buffer, num_registros):
    print(f"\nSubiendo a S3...")   
    try:
        s3_client = boto3.client('s3', region_name=REGION)
        today = datetime.now()
        year = today.strftime('%Y')
        month = today.strftime('%m')
        day = today.strftime('%d')
        timestamp = today.strftime('%Y%m%d_%H%M%S')
        s3_key = f'raw/alumnos/year={year}/month={month}/day={day}/alumnos_{timestamp}.parquet'
        
        print(f"Destino: s3://{BUCKET_NAME}/{s3_key}")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream',
            StorageClass='STANDARD',
            Metadata={
                'source': 'downloads',
                'records': str(num_registros),
                'ingestion_date': today.isoformat(),
                'format': 'parquet',
                'compression': 'snappy'
            }
        )
        
        print(f"Archivo subido exitosamente")
        
        # Verificar
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        print(f"\nVerificación:")
        print(f"   • Tamaño en S3: {response['ContentLength'] / 1024:.2f} KB")
        print(f"   • Storage Class: {response.get('StorageClass', 'STANDARD')}")
        print(f"   • ETag: {response['ETag']}")
        
        return s3_key
        
    except Exception as e:
        print(f"ERROR al subir a S3: {str(e)}")
        return None
# Cost  Opti : Limpia archivos de alumnos de más de 7 días
def limpiar_archivos_antiguos():

    print(f"\nLimpiando archivos antiguos (>7 días)...")
    
    try:
        s3_client = boto3.client('s3', region_name=REGION)
        
        # Listar objetos en el prefijo de alumnos
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix='raw/alumnos/'
        )
        
        if 'Contents' not in response:
            print("   No hay archivos para limpiar")
            return
        
        now = datetime.now()
        deleted_count = 0
        
        for obj in response['Contents']:
            age_days = (now - obj['LastModified'].replace(tzinfo=None)).days
            
            if age_days > 7:
                s3_client.delete_object(Bucket=BUCKET_NAME, Key=obj['Key'])
                deleted_count += 1
                print(f" Eliminado: {obj['Key']} (antigüedad: {age_days} días)")
        
        if deleted_count == 0:
            print(f"No hay archivos antiguos")
        else:
            print(f"Eliminados {deleted_count} archivo(s)")
            
    except Exception as e:
        print(f"Error en limpieza: {str(e)}")

def main():
    print("ETL ORQUESTADO: ALUMNOS.CSV → S3 PARQUET")
    print(f"Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Bucket destino: {BUCKET_NAME}")
    print(f"Región: {REGION}")

    csv_path = buscar_archivo_csv()
    if not csv_path:
        sys.exit(1)
    
    parquet_buffer, num_registros = convertir_csv_a_parquet(csv_path)
    if not parquet_buffer:
        sys.exit(1)
    
    s3_key = subir_a_s3_particionado(parquet_buffer, num_registros)
    if not s3_key:
        sys.exit(1)
    
    limpiar_archivos_antiguos()
    
    print("ETL COMPLETADO EXITOSAMENTE")
    print(f"\nQué hicimos?:")
    print(f"   • Registros procesados: {num_registros:,}")
    print(f"   • Archivo origen: {csv_path}")
    print(f"   • Archivo destino: s3://{BUCKET_NAME}/{s3_key}")
    print(f"\nVer en consola:")
    print(f"   https://s3.console.aws.amazon.com/s3/object/{BUCKET_NAME}?prefix={s3_key}")


if __name__ == "__main__":
    main()