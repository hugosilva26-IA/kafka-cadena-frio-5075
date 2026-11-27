# Telemetría de cadena de frío con Apache Kafka

Práctica del módulo **5074 – Sistemas de Big Data** del Curso de Especialización en Inteligencia Artificial y Big Data. En este proyecto he montado un prototipo para recibir la temperatura y la posición GPS de una flota de camiones frigoríficos y detectar en el momento las lecturas que pueden romper la cadena de frío.

La propuesta sustituye el envío de pequeños CSV a HDFS por eventos publicados en Apache Kafka 4.0, desplegado en modo KRaft con Docker Compose. El productor genera telemetría para cuatro camiones y el consumidor registra las alertas cuando la temperatura supera los −18 °C.

## Estructura del proyecto

```text
├── docker-compose.yml
├── requirements.txt
├── README.md
├── src/
│   ├── productor_frio.py
│   └── consumidor_logista.py
└── evidencias/
    ├── E1_topic_describe.png
    ├── E2_productor_salida.png
    ├── E3-B.png
    ├── E3A_group_antes_fallo.txt
    ├── E3B_group_despues_fallo.txt
    └── alertas_detectadas.log
```

El enunciado llama `consumidor_alertas.py` al consumidor; en este repositorio el archivo equivalente se llama `src/consumidor_logista.py`.

## Requisitos

- Docker Desktop con Docker Compose v2.
- Python 3.10 o superior.
- GitHub, si se va a entregar el repositorio allí.

## Preparar y ejecutar Kafka

Abre PowerShell en la carpeta raíz del proyecto. Crea un entorno virtual e instala la dependencia:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Levanta Kafka y comprueba que el contenedor está activo:

```powershell
docker compose up -d
docker compose ps
docker compose logs kafka
```

Crea el topic con tres particiones y factor de replicación uno. La opción `--if-not-exists` permite repetir el comando si el topic ya está creado:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic telemetria-frio --partitions 3 --replication-factor 1
```

Para revisar sus metadatos:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic telemetria-frio
```

## Ejecutar la prueba

Usa tres terminales de PowerShell, todas situadas en la raíz del proyecto. En las dos primeras, activa el entorno virtual y arranca una instancia del consumidor:

```powershell
.\.venv\Scripts\Activate.ps1
python src\consumidor_logista.py
```

En la tercera terminal, consulta cómo se han repartido las particiones entre las dos instancias:

```powershell
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group grupo-alertas-frio
```

Después de guardar esta salida como evidencia E3-A, ejecuta el productor en esa terminal:

```powershell
python src\productor_frio.py
```

Cuando haya enviado entre 25 y 30 mensajes, detén una de las dos instancias consumidoras con `Ctrl+C`. La instancia que queda debe asumir las particiones de la que se ha detenido. Cuando termine el productor, vuelve a ejecutar el comando de inspección del grupo y guarda la salida como E3-B. En esta comprobación, el grupo debe tener un único consumidor activo y el `LAG` debe ser 0.

El productor publica 60 eventos con una pausa de medio segundo. Inyecta ocho anomalías, repartidas entre los camiones; el consumidor imprime cada lectura, avisa si `temp_c > -18.0` y añade la alerta al log. Para las nuevas ejecuciones, el log conserva las líneas anteriores porque se abre en modo append.

## Evidencias

- **E1 – Topic y particiones:** [captura del topic](evidencias/E1_topic_describe.png).
- **E2 – Productor:** [captura de la salida](evidencias/E2_productor_salida.png), con los eventos, las anomalías y el resumen del particionado.
- **E3-A – Antes de detener un consumidor:** [salida del grupo](evidencias/E3A_group_antes_fallo.txt). Se ven dos `CONSUMER-ID` repartiendo las tres particiones.
- **E3-B – Después de detenerlo:** [salida del grupo](evidencias/E3B_group_despues_fallo.txt) y [captura](evidencias/E3-B.png). Un consumidor queda asignado a las tres particiones y el `LAG` es 0.
- **E4 – Alertas:** [log generado](evidencias/alertas_detectadas.log). En la evidencia guardada hay ocho líneas de alerta.

## Análisis arquitectónico

### HDFS y el problema de los ficheros pequeños

En la arquitectura anterior, cada CSV pequeño creaba un fichero nuevo en HDFS. El NameNode mantiene metadatos de cada fichero y de sus bloques; al acumularse muchos ficheros diminutos, esa carga de metadatos crece aunque el volumen de datos de cada fichero sea bajo [2]. Kafka recibe los eventos de forma continua y los añade a logs particionados, por lo que no hace falta crear un fichero HDFS por cada lectura [1]. Así se reduce el problema de *small files* durante la ingestión y se gana disponibilidad para consumir los datos en cuanto llegan. Kafka no sustituye el almacenamiento histórico de HDFS: en una fase posterior, Spark puede leer el topic y guardar conjuntos de datos en Parquet.

### Clave, orden y paralelismo

El productor usa el identificador del camión (`vehiculo`) como `key`. Kafka dirige siempre una misma clave a la misma partición; de este modo, las lecturas de cada camión mantienen su orden dentro de esa partición [1]. Si se eliminase la clave, Kafka dejaría de garantizar esa asignación estable por vehículo y sus lecturas podrían terminar en particiones diferentes, perdiéndose el orden cronológico por camión al consumir en paralelo. Con tres particiones, una cuarta instancia del mismo consumer group no tendría una partición que procesar y quedaría inactiva; para aprovecharla habría que aumentar el número de particiones.

### Conexión futura desde Spark

Ahora los clientes Python del anfitrión se conectan a `localhost:9092`. Para que también se conecte un contenedor Spark en la red de Docker, configuraría listeners distintos para el anfitrión y para los contenedores [1]. El enunciado menciona `kafka:9092`, pero el ejemplo preparado en este `docker-compose.yml` reserva el puerto interno `29092`; por eso el endpoint anunciado para los contenedores es `kafka:29092`, mientras que el externo conserva `localhost:9092`:

```yaml
KAFKA_LISTENERS: PLAINTEXT://:29092,PLAINTEXT_HOST://:9092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

Así el cliente local sigue usando `localhost:9092` y Spark debe usar `kafka:29092`. El nombre `kafka` resuelve dentro de la red de Compose; `localhost` dentro del contenedor Spark señalaría al propio contenedor Spark, no al broker.

## Referencias

[1] Apache Software Foundation, “Apache Kafka Documentation,” *Apache Kafka*. [En línea]. Disponible en: <https://kafka.apache.org/documentation/>. [Accedido: 27-nov-2026].

[2] Apache Software Foundation, “HDFS Architecture,” *Apache Hadoop Documentation*. [En línea]. Disponible en: <https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html>. [Accedido: 27-nov-2026].
