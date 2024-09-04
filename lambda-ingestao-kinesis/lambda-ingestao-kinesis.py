import json
import requests
import boto3
from datetime import datetime

# Configurações
API_KEY = '98196fde09d81a5ecbf33ef899764f8c'
BASE_URL = 'http://pro.openweathermap.org/data/2.5/weather'
KINESIS_STREAM_NAME = 'meu-primeiro-stream'
REGION_NAME = 'us-east-1'  # Substitua pela sua região desejada

# Inicializar cliente Kinesis
kinesis_client = boto3.client('kinesis', 
                              region_name=REGION_NAME,
                              endpoint_url='http://localhost:4566',  # Use isso para LocalStack
                              aws_access_key_id='test',              # Credenciais fictícias para LocalStack
                              aws_secret_access_key='test')

def load_capitals(filename='capitals.json'):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data['capitals']

def get_weather_data(city, country):
    url = f"{BASE_URL}?q={city},{country}&APPID={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao obter dados para {city}, {country}: {response.status_code}")
        return None

def send_to_kinesis(data):
    kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey=str(data['id'])
    )

def lambda_handler(event, context):
    capitals = load_capitals()
    
    for capital in capitals:
        data = get_weather_data(capital['city'], capital['country'])
        if data:
            send_to_kinesis(data)
            print(f"Dados enviados para Kinesis: {capital['city']}, {capital['country']}")

    return {
        'statusCode': 200,
        'body': json.dumps('Coleta de dados concluída com sucesso!')
    }