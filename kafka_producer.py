import json
import time
import random
import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'sensor_data'
ciudades = ["BOGOTA", "MEDELLIN", "CALI", "BARRANQUILLA", "CARTAGENA", "BUCARAMANGA"]

print("Iniciando generador de datos de remesas con rutas en tiempo real...")

try:
    while True:
        # Seleccionamos origen y destino al azar asegurando que no sean la misma ciudad
        origen = random.choice(ciudades)
        destino = random.choice([c for c in ciudades if c != origen])

        datos_remesa = {
            "id_viaje": random.randint(1000, 9999),
            "origen": origen,
            "destino": destino,
            "cantidad_kg": random.uniform(500.0, 35000.0),
            "horas_viaje": random.uniform(1.0, 48.0),
            "valor_pagado": random.uniform(150000.0, 5000000.0),
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        producer.send(topic_name, value=datos_remesa)
        print(f"Registro enviado: {origen} hacia {destino} por $ {int(datos_remesa['valor_pagado'])}")
        time.sleep(2)
        
except KeyboardInterrupt:
    print("Transmisión detenida por el usuario.")
finally:
    producer.close()