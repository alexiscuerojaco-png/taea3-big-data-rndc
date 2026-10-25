from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Inicialización de la Sesión de Spark
spark = (
        SparkSession.builder\
        .appName("EDA_RNDC_Remesas_Agosto_2026")\
        .getOrCreate()
)

# Reducir registros innecesarios en la consola spark.sparkContext.setLogLevel("ERROR")


# 2. Ruta del archivo en HDFS
file_path =file_path = 'hdfs://localhost:9000/Tarea3/RemesasRNDC_202608.txt'

# 3. Lectura optimizada del archivo
df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .option("delimiter", "|")\
        .load(file_path)

print("\n" + "="*70)
print(" 1. INSPECCIÓN GENERAL Y DIMENSIÓN DEL DATASET")
print("="*70)

total_registros = df.count()
total_columnas = len(df.columns)

print(f"Total de registros procesados: {total_registros:,}")
print(f"Total de columnas: {total_columnas}\n")

print("--- Esquema de Tipos de Datos (Schema) ---")
df.printSchema()

print("\n--- Vista previa de las primeras 5 filas ---")
df.show(5, truncate=True)

print("\n" + "="*70)
print(" 2. CALIDAD DE DATOS (CONTEO DE NULOS)")
print("="*70)

# Revisa nulos en las primeras columnas
null_counts = df.select([F.count(F.when(F.col(c).isNull() | F.isnan(c),
c)).alias(c) for c in df.columns[:6]])
null_counts.show()

print("\n" + "="*70)
print(" 3. DEMOSTRACIÓN DE OPERACIONES EN RDDS")
print("="*70)

#Convertir el DataFrame a RDD
rdd_remesas = df.rdd
print(f"Total de elementos en RDD: {rdd_remesas.count():,}")

print("\n--- Muestra de 2 registros desde RDD ---")
for registro in rdd_remesas.take(2):
        print(registro)

# Cierre de la sesión
spark.stop()