"""
Productor de telemetria de la flota FrioSur Logistica.

Publica eventos JSON en el topic 'telemetria-frio' usando el campo
'vehiculo' como clave de particion, de modo que todas las lecturas de un
mismo camion caen siempre en la misma particion (orden cronologico por camion).
"""

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# ---------------------------------------------------------------- Configuracion del productor
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telemetria-frio"
VEHICULOS = ["CAM-01", "CAM-02", "CAM-03", "CAM-04"]

TOTAL_EVENTOS = 60          # minimo exigido: 60
PAUSA_SEGUNDOS = 0.5        # pausa entre envios
ANOMALIAS_POR_CAMION = 2    # 4 camiones x 2 = 8 anomalias (rango exigido: 6-10)

UMBRAL_ALERTA = -18.0       # temp_c > -18.0 => rotura de cadena de frio
TEMP_NORMAL = (-22.0, -18.5)
TEMP_ANOMALA = (-17.5, -14.0)

# Punto de partida GPS (Sevilla) y desplazamiento inicial distinto por camion
LAT_BASE, LON_BASE = 37.3891, -5.9845

## Esta función crea y configura el productor de Kafka.
def crear_productor() -> KafkaProducer:
    """Productor fiable: acks='all', clave en UTF-8 y valor en JSON."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",#Significa que Kafka debe confirmar el mensaje cuando las réplicas necesarias hayan reconocido su escritura
        key_serializer=lambda k: k.encode("utf-8"),#Convierte la clave del mensaje a bytes usando UTF-8
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),#Convierte el valor del mensaje a JSON y luego a bytes usando UTF-8
    )


def elegir_indices_anomalos() -> set:
    """
    Elige los eventos (por indice) que seran anomalos garantizando que
    queden repartidos: ANOMALIAS_POR_CAMION anomalias en cada vehiculo.
    El camion de cada evento es VEHICULOS[i % 4].
    """
    indices = set()
    for pos_camion in range(len(VEHICULOS)):
        propios = [i for i in range(TOTAL_EVENTOS) if i % len(VEHICULOS) == pos_camion]
        indices.update(random.sample(propios, ANOMALIAS_POR_CAMION))
    return indices


def generar_evento(vehiculo: str, posicion: dict, anomalo: bool) -> dict:
    """Construye el JSON obligatorio del enunciado."""
    # Movimiento GPS pequeño y aleatorio para simular el trayecto
    posicion["lat"] += random.uniform(-0.0005, 0.0005)
    posicion["lon"] += random.uniform(-0.0005, 0.0005)

    rango = TEMP_ANOMALA if anomalo else TEMP_NORMAL
    return {
        "vehiculo": vehiculo,
        "temp_c": round(random.uniform(*rango), 1),
        "lat": round(posicion["lat"], 4),
        "lon": round(posicion["lon"], 4),
        "timestamp_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main() -> None:
    productor = crear_productor()
    indices_anomalos = elegir_indices_anomalos()

    # Estado GPS de cada camion (cada uno arranca algo separado del resto)
    posiciones = {
        v: {"lat": LAT_BASE + n * 0.01, "lon": LON_BASE + n * 0.01}
        for n, v in enumerate(VEHICULOS)
    }

    anomalias_emitidas = 0
    particion_por_camion = {}  # para comprobar el enrutamiento determinista

    print(f"Enviando {TOTAL_EVENTOS} eventos a '{TOPIC}' en {BOOTSTRAP_SERVERS}...\n")

    try:
        for i in range(TOTAL_EVENTOS):
            vehiculo = VEHICULOS[i % len(VEHICULOS)]
            evento = generar_evento(vehiculo, posiciones[vehiculo], i in indices_anomalos)

            # key = vehiculo -> decide la particion; value = JSON del evento
            futuro = productor.send(TOPIC, key=vehiculo, value=evento)
            # .get() espera la confirmacion del broker y devuelve particion y offset
            meta = futuro.get(timeout=10)

            if evento["temp_c"] > UMBRAL_ALERTA:
                anomalias_emitidas += 1

            particion_por_camion.setdefault(vehiculo, set()).add(meta.partition)

            marca = "  <-- ANOMALIA" if evento["temp_c"] > UMBRAL_ALERTA else ""
            print(
                f"[{i + 1:02d}/{TOTAL_EVENTOS}] {vehiculo} | "
                f"temp={evento['temp_c']:6.1f} C | "
                f"particion={meta.partition} | offset={meta.offset}{marca}"
            )
            time.sleep(PAUSA_SEGUNDOS)
    finally:
        productor.flush()
        productor.close()

    print("\n" + "=" * 55)
    print("RESUMEN")
    print(f"  Eventos enviados      : {TOTAL_EVENTOS}")
    print(f"  Anomalias emitidas    : {anomalias_emitidas}")
    print("  Particion por camion  :")
    for vehiculo in VEHICULOS:
        print(f"    {vehiculo} -> particion(es) {sorted(particion_por_camion[vehiculo])}")
    print("=" * 55)


if __name__ == "__main__":
    main()
