import time
import json
import random
from kafka import KafkaProducer

# Configuración del productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "timestamp": int(time.time())
    }

print("Iniciando productor de Kafka. Presiona Ctrl+C para detener.")
try:
    while True:
        sensor_data = generate_sensor_data()
        producer.send('sensor_data', value=sensor_data)
        print(f"Enviado: {sensor_data}")
        time.sleep(1) # Envía un dato cada segundo
except KeyboardInterrupt:
    print("\nProductor detenido.")
    producer.close()