import json
import os
import time
from kafka import KafkaConsumer


# Ruta del log calculada a partir de este script, para que funcione lo lances desde donde lo lances.
RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARPETA_EVIDENCIAS = os.path.join(RAIZ, "evidencias")
RUTA_LOG = os.path.join(CARPETA_EVIDENCIAS, "alertas_detectadas.log")
# Creamos la carpeta evidencias si todavía no existe.
os.makedirs(CARPETA_EVIDENCIAS, exist_ok=True)

# Avisamos por consola de que el consumidor está listo para recibir datos.
print("Escuchando telemetría de cadena de frío...")

# Abrimos el log en modo append ("a") para no borrar alertas de ejecuciones anteriores.
with open(RUTA_LOG, "a", encoding="utf-8") as log:
    while True:
        # Creamos el consumidor y le indicamos de qué topic debe leer los mensajes.
        consumer = KafkaConsumer(
            "telemetria-frio",
            # Dirección del broker Kafka al que se conecta el consumidor.
            bootstrap_servers="localhost:9092",
            # Los consumidores con este mismo grupo comparten el trabajo.
            group_id="grupo-alertas-frio",
            # Si el grupo no tiene una posición guardada, empezamos por el mensaje más antiguo.
            auto_offset_reset="earliest",
            # Desactivamos la confirmación automática para confirmar después de procesar.
            enable_auto_commit=False,
            # Dejamos que kafka-python negocie la versión del protocolo con el broker.
            # La clave llega en bytes; la convertimos a texto y mantenemos None si no existe.
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            # El valor llega en bytes con formato JSON; lo convertimos a un diccionario.
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )

        try:
            # Este bucle queda a la espera y procesa cada mensaje que llega desde Kafka.
            for mensaje in consumer:
                # El contenido del mensaje ya está convertido a datos de Python.
                datos = mensaje.value
                # Mostramos información útil para seguir el mensaje y revisar la temperatura.
                print(
                    f"Recibido -> Partición={mensaje.partition} | Offset={mensaje.offset} | "
                    f"Vehículo={mensaje.key} | Temp={datos['temp_c']} °C"
                )

                # Si la temperatura supera el límite de -18 °C, es una alerta.
                if datos["temp_c"] > -18.0:
                    # Aviso destacado en pantalla.
                    print("!" * 60)
                    print(f"!!! ALERTA ROTURA DE FRÍO: {mensaje.key} a {datos['temp_c']} °C")
                    print("!" * 60)
                    # Escribimos una línea con todos los datos del evento, incluyendo partición y offset.
                    log.write(
                        f"{datos['timestamp_iso']} | vehiculo={datos['vehiculo']} | "
                        f"temp_c={datos['temp_c']} | lat={datos['lat']} | lon={datos['lon']} | "
                        f"particion={mensaje.partition} | offset={mensaje.offset}\n"
                    )
                    # flush() fuerza la escritura a disco ANTES de confirmar el offset.
                    log.flush()

                # Confirmamos el mensaje solo después de procesarlo y guardar la alerta; así Kafka
                # puede volver a entregarlo si el consumidor se detiene antes de llegar a este punto.
                consumer.commit()
        except KeyboardInterrupt:
            print("\nConsumidor detenido por el usuario.")
            break
        except ValueError as error:
            # En Windows puede ocurrir un fallo transitorio del socket dentro de kafka-python-ng.
            if "Invalid file descriptor" not in str(error):
                raise
            print("Conexión Kafka interrumpida; recreando el consumidor en 2 segundos...")
            time.sleep(2)
        finally:
            # Salimos del grupo de forma limpia antes de reconectar o terminar.
            try:
                consumer.close()
            except ValueError as error:
                if "Invalid file descriptor" not in str(error):
                    raise
