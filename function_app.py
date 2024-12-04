import azure.functions as func
import logging
import requests
import json
import os
from azure.eventhub import EventHubProducerClient, EventData
 
# Crear una instancia de la aplicación de funciones
app = func.FunctionApp()
 
# Trigger del Timer, se ejecutará cada 5 minutos
@app.timer_trigger(arg_name="mytimer", schedule="*/5 * * * *")  # Cada 5 minutos
def streaming_weather_timer(mytimer: func.TimerRequest):
    logging.info('Python Timer trigger function ran at %s', mytimer.schedule_status.last)
 
    # Lógica para consumir la API de OpenWeatherMap
    weather_api_url = "http://api.openweathermap.org/data/2.5/weather"
    api_key = os.getenv('API_KEY')  # Obtenemos la API Key de las variables de entorno
    location = "Madrid"  # Puedes cambiar la ubicación según tu interés
 
    # Realizar la solicitud a la API de OpenWeatherMap
    params = {
        'q': location,
        'appid': api_key,
        'units': 'metric'  # Definimos que los datos estén en unidades métricas (Celsius)
    }
    response = requests.get(weather_api_url, params=params)
 
    if response.status_code == 200:
        weather_data = response.json()
        logging.info(f"Datos obtenidos de OpenWeatherMap: {weather_data}")
 
        # Crear un productor para enviar los datos a Event Hub
        event_hub_conn_str = os.getenv('EVENT_HUB_CONNECTION_STRING')
        event_hub_name = "streamingevents"  # Nombre del Event Hub definido en Azure
 
        producer = EventHubProducerClient.from_connection_string(conn_str=event_hub_conn_str, eventhub_name=event_hub_name)
       
        with producer:
            event_data = EventData(json.dumps(weather_data))
            producer.send_batch([event_data])
       
        logging.info("Datos del clima enviados a Event Hub correctamente")
    else:
        logging.error(f"Error al obtener los datos de OpenWeatherMap: {response.status_code}")
 
# Trigger del Event Hub, procesará los eventos recibidos
@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="streamingevents",
                               connection="EVENT_HUB_CONNECTION_STRING")
def streaming_weather_trigger(azeventhub: func.EventHubEvent):
    logging.info('Python EventHub trigger processed an event: %s',
                 azeventhub.get_body().decode('utf-8'))