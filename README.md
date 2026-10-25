# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka
Repositorio para el desarrollo de la Tarea 3 del curso de Big Data de la UNAD.

## Descripción de la solución
Este proyecto implementa dos modalidades de procesamiento de grandes volúmenes de datos utilizando el conjunto de datos de transporte de carga (RNDC):
1. **Procesamiento Batch**: Limpieza, transformación y análisis exploratorio mediante DataFrames y RDDs en Apache Spark.
2. **Procesamiento en Tiempo Real**: Simulación de llegada de datos mediante un productor de Kafka y consumo continuo agrupado por ventanas de tiempo con Spark Streaming.

## Instrucciones de ejecución

### 1. Ejecutar el análisis en Batch
Coloca el dataset en el sistema de archivos HDFS y ejecuta el script principal con Spark:
```bash
python3 eda_rndc.py
