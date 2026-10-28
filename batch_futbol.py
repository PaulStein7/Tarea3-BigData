from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, desc

# 1. Iniciar SparkSession
spark = SparkSession.builder \
    .appName("AnalisisFutbolUNAD") \
    .getOrCreate()

# 2. Cargar el dataset
df = spark.read.csv("results.csv", header=True, inferSchema=True)

print("--- Vista previa de los datos ---")
df.show(5)

# 3. Limpieza: Eliminar filas con valores nulos en las columnas de goles
df_limpio = df.dropna(subset=["home_score", "away_score"])

# 4. Análisis 1: Ventaja de localía (comparando goles)
df_limpio = df_limpio.withColumn("resultado", 
    when(col("home_score") > col("away_score"), "Victoria_Local")
    .when(col("home_score") < col("away_score"), "Victoria_Visitante")
    .otherwise("Empate")
)

print("--- Estadísticas de Ventaja de Localía ---")
df_limpio.groupBy("resultado").count().orderBy(col("count").desc()).show()

# 5. Análisis 2: Promedio de goles por torneo (Top 5)
df_limpio = df_limpio.withColumn("goles_totales", col("home_score") + col("away_score"))

print("--- Promedio de goles por torneo (Top 5) ---")
df_limpio.groupBy("tournament") \
    .agg(avg("goles_totales").alias("promedio_goles"), count("*").alias("total_partidos")) \
    .orderBy(col("total_partidos").desc()).limit(5).show(truncate=False)

spark.stop()