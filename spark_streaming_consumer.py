from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, format_number, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

spark = SparkSession.builder.appName("RNDCStreamingRutas").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id_viaje", IntegerType()),
    StructField("origen", StringType()),
    StructField("destino", StringType()),
    StructField("cantidad_kg", FloatType()),
    StructField("horas_viaje", FloatType()),
    StructField("valor_pagado", FloatType()),
    StructField("timestamp", TimestampType())
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Agrupamos por ventana de tiempo origen y destino para calcular el promedio exacto[cite: 3]
estadisticas_rutas = parsed_df.groupBy(window(col("timestamp"), "1 minute"), col("origen"), col("destino")) \
    .agg(avg("valor_pagado").alias("promedio_pagado"))

# Damos formato concatenando el simbolo de pesos y usando format_number para poner comas y quitar decimales
resultado_formateado = estadisticas_rutas.withColumn(
    "valor_promedio_pesos",
    concat(lit("$ "), format_number(col("promedio_pagado"), 0))
).drop("promedio_pagado")

# option("truncate", "false") evita que Spark corte el texto de las columnas largas en la pantalla
query = resultado_formateado.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()