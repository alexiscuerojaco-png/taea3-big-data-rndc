# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka 🚚📊

Este repositorio contiene el desarrollo del componente práctico de la **Tarea 3 del curso de Big Data de la Universidad Nacional Abierta y a Distancia (UNAD)**. El proyecto se centra en el análisis de datos logísticos del Registro Nacional de Despachos de Carga (RNDC) de Colombia, aplicando técnicas de procesamiento por lotes (Batch) y en tiempo real (Streaming).

## 📌 Contexto del Proyecto

El sector logístico y de transporte requiere procesar grandes volúmenes de información para la toma de decisiones. En este proyecto se utilizan tecnologías del ecosistema Big Data para analizar los despachos de carga, evaluar costos y monitorizar rutas en tiempo real. 

### Arquitectura Tecnológica Utilizada:
*   **Hadoop (HDFS):** Almacenamiento distribuido para el dataset inicial.
*   **Apache Spark:** Motor de procesamiento distribuido para análisis exploratorio (DataFrames y RDDs).
*   **Apache Kafka:** Plataforma de transmisión de eventos para simular la llegada de datos en vivo.
*   **Spark Streaming:** Módulo de Spark para consumir y analizar el flujo de datos en tiempo real.
*   **Python (PySpark):** Lenguaje principal de desarrollo.

---

## 📂 Descripción de los Archivos del Repositorio

El proyecto se divide en tres scripts fundamentales:

### 1. `eda_rndc.py` (Procesamiento Batch)
**¿Para qué sirve?**
Realiza el Análisis Exploratorio de Datos (EDA) sobre el dataset histórico de remesas almacenado en el sistema HDFS. 
**¿Qué hace internamente?**
*   Se conecta al sistema de archivos distribuido (`hdfs://localhost:9000/Tarea3/`).
*   Infiere el esquema y carga los millones de registros del RNDC en un DataFrame de Spark.
*   Realiza tareas de limpieza (conteo de valores nulos).
*   Aplica transformaciones y filtros (ej. viajes con valores específicos).
*   Demuestra la interoperabilidad entre DataFrames y RDDs para el procesamiento distribuido profundo.

### 2. `kafka_producer.py` (Simulador de Datos en Tiempo Real)
**¿Para qué sirve?**
Actúa como un "Producer" de Kafka, generando y emitiendo datos sintéticos que imitan la operación logística diaria.
**¿Qué hace internamente?**
*   Se conecta al broker local de Kafka en el puerto `9092`.
*   Mediante un ciclo infinito, genera diccionarios JSON con información aleatoria: origen, destino, kilos transportados, horas de viaje y valor pagado.
*   Asigna un `timestamp` exacto al momento de la creación.
*   Envía estos paquetes al topic `sensor_data` cada 2 segundos.

### 3. `spark_streaming_consumer.py` (Procesamiento y Analítica Streaming)
**¿Para qué sirve?**
Actúa como "Consumer" del topic de Kafka, ingiriendo el flujo de datos para aplicar cálculos estadísticos matemáticos sobre la marcha sin guardar los datos en disco.
**¿Qué hace internamente?**
*   Se suscribe al topic `sensor_data` de Kafka.
*   Parsea la estructura binaria JSON entrante aplicando un esquema estructurado (StructType) para reconocer los campos (rutas, pesos, dinero).
*   Agrupa la información usando una "Ventana de Tiempo" (*Time Window*) de 1 minuto, separada por Origen y Destino.
*   Calcula el promedio del valor pagado y lo formatea a moneda (Pesos sin decimales).
*   Proyecta las tablas actualizadas dinámicamente directo en la terminal.

---

## 🚀 Instrucciones de Ejecución Paso a Paso

### Requisitos Previos
Asegúrese de tener configurada una máquina virtual Linux (Ubuntu) con:
*   Hadoop (HDFS) corriendo en `localhost:9000`.
*   Apache Spark (v3.5.3) y PySpark instalados.
*   Apache Kafka (v3.6.1) descargado en el entorno.
*   Librería instalada: Ejecutar `pip3 install kafka-python`.

### FASE 1: Ejecución del Procesamiento Batch
1. Asegúrese de que su archivo de datos (ej. `rows.csv` o `.txt`) esté cargado en HDFS dentro de la carpeta `/Tarea3/`.
2. Abra una terminal y ejecute el script:
   ```bash
   python3 eda_rndc.py
   ```
3. Observe en la consola las métricas descriptivas, el esquema validado y la muestra de los RDDs.

### FASE 2: Ejecución del Procesamiento en Tiempo Real (Streaming)

**Paso 2.1: Iniciar los servicios base de Kafka**
Abra terminales separadas en su directorio de Kafka y ejecute:
1. Iniciar Zookeeper:
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```
2. Iniciar el servidor Kafka:
   ```bash
   bin/kafka-server-start.sh config/server.properties
   ```
3. Crear el canal (Topic) de comunicación:
   ```bash
   bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

**Paso 2.2: Lanzar el Productor de Remesas**
Abra una nueva terminal en el directorio donde clonó este repositorio y ejecute:
```bash
python3 kafka_producer.py
```
*(Verá mensajes indicando que se están enviando registros de ciudades y costos).*

**Paso 2.3: Lanzar el Consumidor de Spark**
En una terminal final, procese los datos inyectando los paquetes de conexión de Kafka-Spark:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```
*(Spark comenzará a procesar y mostrará en pantalla una tabla agrupada por rutas actualizando los promedios monetarios minuto a minuto).*
