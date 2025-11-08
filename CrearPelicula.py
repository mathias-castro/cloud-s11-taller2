import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    
    # Parsear el body JSON
    try:
        if isinstance(event['body'], str):
            body = json.loads(event['body'])
        else:
            body = event['body']
    except (json.JSONDecodeError, KeyError) as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid JSON body'})
        }
    
    tenant_id = body['tenant_id']
    texto = body['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["BUCKET_NAME"]
    
    # Proceso
    uuidv1 = str(uuid.uuid1())
    timestamp = datetime.utcnow().isoformat()
    
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
          'texto': texto
        },
        'timestamp': timestamp
    }
    
    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)
    
    # Estrategia de Ingesta Push - Guardar JSON en S3
    try:
        s3_client = boto3.client('s3')
        
        # Crear nombre del archivo directamente en la raíz del bucket: tenant_id_uuid.json
        s3_key = f"{tenant_id}_{uuidv1}.json"
        
        # Convertir comentario a JSON
        comentario_json = json.dumps(comentario, ensure_ascii=False, indent=2)
        
        # Subir archivo a S3
        s3_response = s3_client.put_object(
            Bucket=nombre_bucket,
            Key=s3_key,
            Body=comentario_json,
            ContentType='application/json',
            Metadata={
                'tenant_id': tenant_id,
                'uuid': uuidv1,
                'timestamp': timestamp
            }
        )
        
        print(f"Archivo guardado en S3: s3://{nombre_bucket}/{s3_key}")
        
    except Exception as e:
        print(f"Error al guardar en S3: {str(e)}")
        # No fallar la función si hay error en S3, solo registrar el error
        s3_response = {"error": str(e)}
    
    # Salida (json)
    print(comentario)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'dynamodb_response': response,
        's3_response': s3_response,
        's3_location': f"s3://{nombre_bucket}/{s3_key}" if 's3_key' in locals() else None
    }
