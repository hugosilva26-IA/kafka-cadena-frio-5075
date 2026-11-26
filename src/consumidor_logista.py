import json
from kafka import KafkaConsumer


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
    # La clave llega en bytes; la convertimos a texto y mantenemos None si no existe.
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    # El valor llega en bytes con formato JSON; lo convertimos a un diccionario.
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Avisamos por consola de que el consumidor está listo para recibir datos.
print("Escuchando telemetría de cadena de frío...")
# Este bucle queda a la espera y procesa cada mensaje que llega desde Kafka.
for mensaje in consumer:
    # El contenido del mensaje ya está convertido a datos de Python.
    datos = mensaje.value
    # Marcamos como alerta las temperaturas por encima del límite de -18 °C.
    alerta = " [¡ALERTA ROTURA DE FRÍO!]" if datos["temp_c"] > -18.0 else ""
    # Mostramos información útil para seguir el mensaje y revisar la temperatura.
    print(
        f"Recibido -> Partición={mensaje.partition} | Offset={mensaje.offset} | "
        f"Vehículo={mensaje.key} | Temp={datos['temp_c']} °C{alerta}"
    )
    # Confirmamos el mensaje solo después de procesarlo; así Kafka puede volver a
    # entregarlo si el consumidor se detiene antes de llegar a este punto.
    consumer.commit()
