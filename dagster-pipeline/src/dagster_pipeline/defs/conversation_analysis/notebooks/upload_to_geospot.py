"""Script para cargar CSV a Geospot manualmente."""

import pandas as pd
import boto3
import requests
from io import BytesIO

# Configuración
S3_BUCKET_NAME = "dagster-assets-production"
GEOSPOT_API_URL = "https://geospot.spot2.mx/data-lake-house/dagster/"
GEOSPOT_API_KEY_PARAM = "/dagster/API_GEOSPOT_LAKEHOUSE_KEY"

# Nombre del archivo CSV
CSV_FILENAME = "df_conversations_metrics.csv"


def get_ssm_parameter(name: str, decrypt: bool = True) -> str:
    """Get a parameter value from AWS SSM Parameter Store."""
    ssm = boto3.client("ssm", region_name="us-east-1")
    response = ssm.get_parameter(Name=name, WithDecryption=decrypt)
    return response["Parameter"]["Value"]


def main():
    print("=" * 50)
    print("UPLOAD TO GEOSPOT")
    print("=" * 50)
    
    # Verificar credenciales AWS
    print("\n🔐 Verificando credenciales AWS...")
    try:
        sts = boto3.client('sts', region_name='us-east-1')
        identity = sts.get_caller_identity()
        print(f"✅ Autenticado como: {identity['Arn']}")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   Ejecuta 'aws-login' primero")
        return
    
    # Leer CSV
    print(f"\n📄 Leyendo {CSV_FILENAME}...")
    try:
        df = pd.read_csv(CSV_FILENAME)
        print(f"✅ Leído {len(df)} registros")
        print(f"📊 Columnas: {list(df.columns)}")
    except FileNotFoundError:
        print(f"❌ Archivo no encontrado: {CSV_FILENAME}")
        print(f"   Asegúrate de que el archivo esté en la carpeta notebooks/")
        return
    
    # Arreglar conv_messages: convertir de formato Python a JSON válido
    if 'conv_messages' in df.columns:
        import ast
        import json
        
        def fix_json(val):
            if pd.isna(val):
                return None
            try:
                # Si ya es JSON válido, parsearlo y re-serializarlo
                return json.dumps(json.loads(str(val)))
            except:
                try:
                    # Si es formato Python (comillas simples), convertir
                    parsed = ast.literal_eval(str(val))
                    return json.dumps(parsed)
                except:
                    return str(val)
        
        print("\n🔧 Arreglando formato JSON de conv_messages...")
        df['conv_messages'] = df['conv_messages'].apply(fix_json)
        print("✅ Formato JSON corregido")
    
    # Subir a S3
    print(f"\n☁️ Subiendo a S3...")
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_key = f'conversation_analysis/{CSV_FILENAME}'
    
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType='text/csv'
    )
    print(f"✅ Subido a s3://{S3_BUCKET_NAME}/{s3_key}")
    
    # Triggear Geospot
    print(f"\n🚀 Triggeando carga a Geospot...")
    api_key = get_ssm_parameter(GEOSPOT_API_KEY_PARAM)
    
    headers = {
        "Authorization": f"Api-Key {api_key}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "bucket_name": S3_BUCKET_NAME,
        "s3_key": s3_key,
        "table_name": "bt_conv_conversations_bck",
        "mode": "replace",
    }
    
    response = requests.post(
        GEOSPOT_API_URL,
        headers=headers,
        json=payload,
        timeout=120,
    )
    
    print(f"📥 Respuesta: {response.status_code}")
    print(f"   {response.json()}")
    
    if response.status_code == 200:
        print("\n✅ ¡Carga iniciada exitosamente!")
    else:
        print("\n❌ Error en la carga")


if __name__ == "__main__":
    main()

