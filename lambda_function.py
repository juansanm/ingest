import json
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
s3_client = boto3.client('s3')
BUCKET_NAME = 'etl-aws-sanmiguel'
SOURCE_BUCKET = 'etl-aws-sanmiguel'
SOURCE_KEY = 'uploads/alumnos.csv'

def lambda_handler(event, context):   
    print(f"Iniciando ETL - {datetime.now().isoformat()}")
    print(f"Event: {json.dumps(event)}")    
    try:
        print(f"\nDescargando {SOURCE_KEY} desde S3...")
        csv_obj = s3_client.get_object(
            Bucket=SOURCE_BUCKET,
            Key=SOURCE_KEY
        )      
        csv_content = csv_obj['Body'].read()
        print(f"CSV descargado: {len(csv_content)} bytes")
        print(f"\nProcesando CSV...")
        df = pd.read_csv(BytesIO(csv_content))
        df = df.convert_dtypes()       
        num_registros = len(df)
        print(f"Registros procesados: {num_registros:,}")
        print(f"   Columnas: {', '.join(df.columns.tolist())}")
        print(f"\nConvirtiendo a Parquet...")
        parquet_buffer = BytesIO()
        df.to_parquet(
            parquet_buffer,
            engine='pyarrow',
            compression='snappy',
            index=False
        )        
        parquet_buffer.seek(0)
        parquet_size = len(parquet_buffer.getvalue())        
        print(f"Parquet generado: {parquet_size / 1024:.2f} KB")
        today = datetime.now()
        year = today.strftime('%Y')
        month = today.strftime('%m')
        day = today.strftime('%d')
        timestamp = today.strftime('%Y%m%d_%H%M%S')       
        s3_key = f'raw/alumnos/year={year}/month={month}/day={day}/alumnos_{timestamp}.parquet'
        print(f"\nSubiendo a s3://{BUCKET_NAME}/{s3_key}")
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream',
            StorageClass='STANDARD',
            Metadata={
                'source': 'lambda_etl',
                'records': str(num_registros),
                'ingestion_date': today.isoformat(),
                'format': 'parquet',
                'compression': 'snappy'
            }
        )        
        print(f"✅ Archivo subido exitosamente")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'ETL completado exitosamente',
                'records_processed': num_registros,
                's3_key': s3_key,
                'timestamp': today.isoformat()
            })
        }        
    except Exception as e:
        print(f"ERROR triste: {str(e)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error en ETL',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }