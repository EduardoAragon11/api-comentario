import boto3
import uuid
import os
import csv
from io import StringIO

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'text': texto
    }
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)
    # Salida (json)
    print(comentario)
    
    ## Create .csv
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow([comentario['tenant_id'], comentario['uuid'], comentario['text']])
    
    ### Upload to bucket
    s3_client = boto3.client("s3")
    
    nombre_bucket = os.environ["BUCKET_NAME"]

    s3_client.put_object(
        Bucket=nombre_bucket,
        Key=f"comentario-{comentario['uuid']}.csv",
        Body=csv_buffer.getvalue()
    )

    return {
        'statusCode': 200,
        'comentario': comentario,
        'response': response
    }
