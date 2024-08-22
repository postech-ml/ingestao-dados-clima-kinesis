# import json
# import requests
# import boto3
# import time
# from datetime import datetime, timedelta

# # Configurações
# API_KEY = '98196fde09d81a5ecbf33ef899764f8c'
# BASE_URL = 'http://pro.openweathermap.org/data/2.5/weather'
# KINESIS_STREAM_NAME = 'meu-primeiro-stream'
# REGION_NAME = 'us-east-1'

# # Inicializar cliente Kinesis
# kinesis_client = boto3.client('kinesis', region_name=REGION_NAME)

# def load_capitals(filename='capitals.json'):
#     with open(filename, 'r') as file:
#         data = json.load(file)
#     return data['capitals']

# def get_weather_data(city, country):
#     url = f"{BASE_URL}?q={city},{country}&APPID={API_KEY}"
#     response = requests.get(url)
#     if response.status_code == 200:
#         return response.json()
#     else:
#         print(f"Erro ao obter dados para {city}, {country}: {response.status_code}")
#         return None

# def send_to_kinesis(data):
#     kinesis_client.put_record(
#         StreamName=KINESIS_STREAM_NAME,
#         Data=json.dumps(data),
#         PartitionKey=str(data['id'])
#     )

# def main():
#     capitals = load_capitals()
    
#     for capital in capitals:
#         data = get_weather_data(capital['city'], capital['country'])
#         if data:
#             send_to_kinesis(data)
#             print(f"Dados enviados para Kinesis: {capital['city']}, {capital['country']}")
#         time.sleep(1)  # Pequena pausa para não sobrecarregar a API

# if __name__ == "__main__":
#     while True:
#         now = datetime.now()
#         if now.minute == 30:
#             print(f"Iniciando coleta de dados às {now}")
#             main()
#             # Espera até o próximo ciclo (próxima hora)
#             next_run = now.replace(minute=30, second=0, microsecond=0) + timedelta(hours=1)
#             time.sleep((next_run - now).total_seconds())
#         else:
#             # Espera até o próximo minuto 30
#             next_run = now.replace(minute=30, second=0, microsecond=0)
#             if next_run <= now:
#                 next_run += timedelta(hours=1)
#             time.sleep((next_run - now).total_seconds())

import json
import requests
import boto3
import time
from datetime import datetime, timedelta

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

def main():
    capitals = load_capitals()
    
    for capital in capitals:
        data = get_weather_data(capital['city'], capital['country'])
        if data:
            send_to_kinesis(data)
            print(f"Dados enviados para Kinesis: {capital['city']}, {capital['country']}")
        time.sleep(1)  # Pequena pausa para não sobrecarregar a API

if __name__ == "__main__":
    while True:
        now = datetime.now()
        if now.minute % 10 == 5:  # Executa nos minutos 5, 15, 25, 35, 45, 55
            print(f"Iniciando coleta de dados às {now}")
            main()
            # Espera até o próximo ciclo (próximos 10 minutos)
            next_run = now.replace(minute=(now.minute // 10 * 10 + 5) % 60, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(minutes=10)
            time.sleep((next_run - now).total_seconds())
        else:
            # Espera até o próximo minuto de execução
            next_run = now.replace(minute=(now.minute // 10 * 10 + 5) % 60, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(minutes=10)
            time.sleep((next_run - now).total_seconds())
