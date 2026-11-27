# Telemetría de cadena de frío con Apache Kafka

Práctica del módulo **5075 – Sistemas de Big Data** del Curso de Especialización en Inteligencia Artificial y Big Data. El proyecto simula la telemetría de cuatro camiones frigoríficos y detecta en tiempo real las lecturas que superan −18 °C.

Kafka recibe los eventos en el topic `telemetria-frio`. El productor genera y publica los datos; dos instancias del consumidor, dentro del mismo grupo, se reparten las particiones y guardan las alertas en un archivo de log.

## Estructura del proyecto

```text
├── docker-compose.yml
├── requirements.txt
├── README.md
├── src/
│   ├── productor_frio.py
│   └── consumidor_alerta.py
└── evidencias/
    ├── E1_topic_describe.png
    ├── E2_productor_salida.png
    ├── E3-B.png
    ├── E3A_group_antes_fallo.txt
    ├── E3B_group_despues_fallo.txt
    └── alertas_detectadas.log
```




## Qué hace cada archivo

- [`src/productor_frio.py`](src/productor_frio.py) genera 60 eventos JSON. Cada evento contiene el identificador del vehículo, la temperatura, la posición GPS y la fecha. Publica cada evento en Kafka con el identificador del vehículo como clave. Así, todas las lecturas de un camión se enrutan a la misma partición y conservan su orden. Genera ocho temperaturas anómalas (dos por camión), superiores a −18 °C.
- [`src/consumidor_alertas.py`](src/consumidor_alertas.py) lee el topic como parte del grupo `grupo-alertas-frio`. Muestra cada lectura y, si supera −18 °C, imprime una alerta y añade sus datos a `evidencias/alertas_detectadas.log`. Confirma el offset después de procesar el mensaje y guardar la alerta.
- [`docker-compose.yml`](docker-compose.yml) inicia un broker Kafka 4.0 de un nodo en modo KRaft. El broker escucha en `localhost:9092`, que es la dirección usada por ambos scripts.
- [`requirements.txt`](requirements.txt) contiene la dependencia Python para conectar con Kafka.

## Requisitos

- Docker Desktop con Docker Compose v2.
- Python 3.10 o superior.

## Preparación

Ejecuta los comandos desde PowerShell, en la carpeta raíz del proyecto. Crea un entorno virtual e instala la dependencia:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Inicia Kafka y espera a que el contenedor esté activo:

```powershell
docker compose up -d
docker compose ps
docker compose logs kafka
```

Cuando Kafka esté listo, crea el topic con tres particiones. El comando se puede repetir sin error si el topic ya existe:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic telemetria-frio --partitions 3 --replication-factor 1
```

Comprueba la configuración del topic:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic telemetria-frio
```

## Ejecución paso a paso

La prueba de reparto y recuperación usa **tres terminales de PowerShell**. Abre las tres en la raíz del proyecto y activa el entorno virtual en cada una:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 1. Arranca dos consumidores

En las terminales 1 y 2 ejecuta el consumidor:

```powershell
python src\consumidor_alertas.py
```

Ambos procesos usan el grupo `grupo-alertas-frio`. Kafka asignará las tres particiones entre ellos. Déjalos funcionando durante el resto de la prueba.

### 2. Comprueba el reparto inicial

En la terminal 3 consulta el grupo:

```powershell
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group grupo-alertas-frio
```

Ejecuta la consulta hasta que aparezcan los dos consumidores y las tres particiones.

### 3. Publica la telemetría

En la terminal 3 ejecuta el productor:

```powershell
python src\productor_frio.py
```

El productor tarda aproximadamente 30 segundos: envía un evento cada medio segundo y espera la confirmación de Kafka. En la salida verás el vehículo, temperatura, partición y offset asignados. Las lecturas superiores a −18 °C se marcan como anomalías. Al final aparece un resumen con el número de eventos y la partición asignada a cada camión.

Mientras se ejecuta, ambos consumidores muestran los eventos recibidos. Las anomalías también aparecen como alertas en sus terminales y se escriben en `evidencias/alertas_detectadas.log`.

### 4. Detén un consumidor y observa la recuperación

Cuando el productor haya enviado aproximadamente entre 25 y 30 eventos, detén **uno** de los consumidores con `Ctrl+C`. Kafka detectará que ese proceso salió y reasignará sus particiones al consumidor que sigue activo. Deja que el productor termine.

Después, vuelve a consultar el grupo desde la terminal 3:

```powershell
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group grupo-alertas-frio
```

Debe aparecer un consumidor activo con las tres particiones y `LAG` en cero cuando haya terminado de procesar los mensajes.

## Cómo se procesa un mensaje

1. El productor construye un evento JSON con vehículo, temperatura, coordenadas y marca de tiempo UTC.
2. Envía el evento a `telemetria-frio`, usando el vehículo como clave. Kafka calcula la partición a partir de esa clave; por eso un mismo vehículo mantiene sus lecturas en una partición.
3. Kafka entrega los mensajes a los consumidores del grupo. Cada partición se asigna a un único consumidor del grupo a la vez; las instancias comparten el trabajo.
4. El consumidor compara `temp_c` con −18 °C. Si la temperatura es mayor, registra una alerta con la partición y el offset del mensaje.
5. Tras procesar el mensaje, el consumidor confirma el offset. Si un consumidor se detiene, Kafka vuelve a repartir sus particiones y el otro continúa desde el último offset confirmado.

## Evidencias

- **E1 – Topic:** [descripción del topic y sus particiones](evidencias/E1_topic_describe.png).
- **E2 – Productor:** [salida del productor](evidencias/E2_productor_salida.png).
- **E3-A – Reparto inicial:** [salida del grupo antes de detener un consumidor](evidencias/E3A_group_antes_fallo.txt).
- **E3-B – Recuperación:** [salida del grupo después de detenerlo](evidencias/E3B_group_despues_fallo.txt).
- **E4 – Alertas:** [registro de temperaturas fuera del umbral](evidencias/alertas_detectadas.log).

El consumidor abre el log en modo append, por lo que las nuevas ejecuciones añaden líneas al final y no borran las alertas anteriores. Para obtener un log limpio, guarda o elimina el archivo anterior antes de iniciar otra ejecución.

## Análisis arquitectónico

### HDFS y el problema de los ficheros pequeños

En la arquitectura anterior, cada CSV pequeño creaba un fichero nuevo en HDFS. El NameNode mantiene metadatos de cada fichero y de sus bloques; al acumularse muchos ficheros diminutos, esa carga de metadatos crece aunque el volumen de datos de cada fichero sea bajo [2]. Kafka recibe los eventos de forma continua y los añade a logs particionados, por lo que no hace falta crear un fichero HDFS por cada lectura [1]. Así se reduce el problema de *small files* durante la ingestión y se gana disponibilidad para consumir los datos en cuanto llegan. Kafka no sustituye el almacenamiento histórico de HDFS: en una fase posterior, Spark puede leer el topic y guardar conjuntos de datos en Parquet.

### Clave, orden y paralelismo

El productor usa el identificador del camión (`vehiculo`) como `key`. Kafka dirige una misma clave a una misma partición; de este modo, las lecturas de cada camión mantienen su orden dentro de esa partición [1]. Sin esa clave, no se conservaría la asignación estable por vehículo y las lecturas podrían acabar en particiones diferentes, perdiéndose el orden cronológico por camión al consumir en paralelo. Con tres particiones, una cuarta instancia del mismo consumer group no tendría una partición que procesar y quedaría inactiva; para aprovecharla habría que aumentar el número de particiones.

### Conexión futura desde Spark

En esta práctica, los scripts ejecutados en el anfitrión se conectan a `localhost:9092`. El `docker-compose.yml` actual no configura un listener interno para clientes Spark. Para conectar un contenedor Spark en la red de Docker habría que añadir listeners separados para los clientes del anfitrión y los contenedores [1]. Por ejemplo, se podría reservar `29092` para el tráfico interno y mantener `9092` para el anfitrión:

```yaml
KAFKA_LISTENERS: PLAINTEXT://:29092,PLAINTEXT_HOST://:9092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

Con esa configuración, los scripts locales seguirían usando `localhost:9092` y Spark usaría `kafka:29092`. El nombre `kafka` resuelve dentro de la red de Compose; `localhost` dentro del contenedor Spark señalaría al propio contenedor Spark, no al broker.

## Referencias

[1] Apache Software Foundation, “Apache Kafka Documentation,” *Apache Kafka*. [En línea]. Disponible en: <https://kafka.apache.org/documentation/>. [Accedido: 27-nov-2026].

[2] Apache Software Foundation, “HDFS Architecture,” *Apache Hadoop Documentation*. [En línea]. Disponible en: <https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html>. [Accedido: 27-nov-2026].
