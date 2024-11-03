import random
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

wind_directions = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]

# Función para codificar los datos en 3 bytes
def encode_data(temperature, humidity, wind_direction):
    temp_int = int(temperature * 100)  # Escalar temperatura
    wind_index = wind_directions.index(wind_direction)  # Índice de dirección
    encoded_value = (temp_int << 10) | (humidity << 3) | wind_index  # Empaquetar en 24 bits
    return encoded_value.to_bytes(3, byteorder='big')

# Función para decodificar los datos de 3 bytes
def decode_data(encoded_bytes):
    encoded_value = int.from_bytes(encoded_bytes, byteorder='big')
    temp_int = (encoded_value >> 10) & 0x3FFF  # 14 bits para temperatura
    humidity = (encoded_value >> 3) & 0x7F     # 7 bits para humedad
    wind_index = encoded_value & 0x07          # 3 bits para dirección
    temperature = temp_int / 100.0
    wind_direction = wind_directions[wind_index]
    return {
        "temperatura": temperature,
        "humedad": humidity,
        "direccion_viento": wind_direction
    }

class DataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: v  # No se usa JSON aquí, es un payload de bytes
        )
        self.running = True

    def generate_data(self):
        temperatura = round(random.uniform(0, 110), 2)
        humedad = random.randint(0, 100)
        direccion_viento = random.choice(wind_directions)
        print(f"Valores generados antes de codificar: temperatura={temperatura}, humedad={humedad}, direccion_viento={direccion_viento}")
        return encode_data(temperatura, humedad, direccion_viento)

    def run(self):
        while self.running:
            try:
                datos_codificados = self.generate_data()
                self.producer.send(TOPIC, value=datos_codificados)
                print("Datos enviados (codificados):", datos_codificados)
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
            value_deserializer=lambda x: x  # Deserialización directa de bytes
        )
        self.running = True

    def run(self):
        for message in self.consumer:
            if not self.running:
                break
            try:
                data = decode_data(message.value)
                temp_data.append(data["temperatura"])
                humidity_data.append(data["humedad"])
                wind_data.append(data["direccion_viento"])
                timestamps.append(time.time())
                print("Datos recibidos (decodificados):", data)
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
