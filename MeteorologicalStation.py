import random
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from threading import Thread

KAFKA_SERVER = 'lab9.alumchat.lol:9092'
TOPIC = '21169'
GROUP_ID = 'grupo_L&D'

temp_data = []
humidity_data = []
wind_data = []
timestamps = [] 

class DataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.running = True

    def generate_data(self):
        return {
            "temperatura": round(random.uniform(0, 110), 2),
            "humedad": random.randint(0, 100),
            "direccion_viento": random.choice(["N", "NO", "O", "SO", "S", "SE", "E", "NE"])
        }

    def run(self):
        while self.running:
            try:
                datos = self.generate_data()
                self.producer.send(TOPIC, key='sensorLD', value=datos)
                print("Datos enviados:", datos)
                time.sleep(random.randint(15, 30))
            except Exception as e:
                print(f"Error al producir datos: {e}")
                time.sleep(1)

    def stop(self):
        self.running = False

class DataConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            TOPIC,
            group_id=GROUP_ID,
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )
        self.running = True

    def run(self):
        for message in self.consumer:
            if not self.running:
                break
            try:
                data = message.value
                temp_data.append(data["temperatura"])
                humidity_data.append(data["humedad"])
                wind_data.append(data["direccion_viento"])
                timestamps.append(time.time())
                print("Datos recibidos:", data)
            except Exception as e:
                print(f"Error al procesar mensaje: {e}")

    def stop(self):
        self.running = False
        self.consumer.close()

def update_plot(frame):
    try:
        plt.clf()
        
        if timestamps:
            start_time = timestamps[0]
            time_points = [t - start_time for t in timestamps]
        
            # Gráfica de temperatura
            ax1 = plt.subplot(2, 1, 1)
            ax1.plot(time_points, temp_data, 'r-o', label='Temperatura (°C)')
            ax1.set_ylabel('Temperatura (°C)')
            ax1.set_title('Monitoreo en Tiempo Real')
            ax1.set_xlim(max(0, time_points[-1] - 350), time_points[-1])
            ax1.grid(True)
            ax1.legend()

            # Gráfica de humedad
            ax2 = plt.subplot(2, 1, 2)
            ax2.plot(time_points, humidity_data, 'b-o', label='Humedad (%)')
            ax2.set_ylabel('Humedad (%)')
            ax2.set_xlabel('Tiempo (s)')
            ax2.set_xlim(max(0, time_points[-1] - 350), time_points[-1])
            ax2.grid(True)
            ax2.legend()

        plt.tight_layout()

    except Exception as e:
        print(f"Error en actualización de gráfica: {e}")

def main():
    producer = DataProducer()
    consumer = DataConsumer()

    producer_thread = Thread(target=producer.run)
    consumer_thread = Thread(target=consumer.run)

    producer_thread.start()
    consumer_thread.start()

    plt.figure(figsize=(10, 6))
    ani = FuncAnimation(plt.gcf(), update_plot, interval=1000)

    try:
        plt.show()
    except KeyboardInterrupt:
        print("Cerrando aplicación...")
    finally:
        producer.stop()
        consumer.stop()
        producer_thread.join()
        consumer_thread.join()

if __name__ == "__main__":
    main()
