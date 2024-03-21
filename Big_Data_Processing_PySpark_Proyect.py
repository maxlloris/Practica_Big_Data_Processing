# Databricks notebook source
# MAGIC %md
# MAGIC # Big Data Processing - PySpark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# COMMAND ----------

happiness_report_21 = spark.read.option("header", "true").csv("dbfs:/FileStore/big-data-processing-nov-2023/world_happiness_report_2021.csv")

# COMMAND ----------

display(happiness_report_21)

# COMMAND ----------

happiness_report_21.printSchema()

# COMMAND ----------

# Importamos librerias necesarias para este paso
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define el esquema personalizado
happiness_report_21_schema = StructType([
    StructField('Country name', StringType(), True),
    StructField('Regional indicator', StringType(), True),
    StructField('Ladder score', DoubleType(), True),
    StructField('Standard error of ladder score', DoubleType(), True),
    StructField('upperwhisker', DoubleType(), True),
    StructField('lowerwhisker', DoubleType(), True),
    StructField('Logged GDP per capita', DoubleType(), True),
    StructField('Social support', DoubleType(), True),
    StructField('Healthy life expectancy', DoubleType(), True),
    StructField('Freedom to make life choices', DoubleType(), True),
    StructField('Generosity', DoubleType(), True),
    StructField('Perceptions of corruption', DoubleType(), True),
    StructField('Ladder score in Dystopia', DoubleType(), True),
    StructField('Explained by: Log GDP per capita', DoubleType(), True),
    StructField('Explained by: Social support', DoubleType(), True),
    StructField('Explained by: Healthy life expectancy', DoubleType(), True),
    StructField('Explained by: Freedom to make life choices', DoubleType(), True),
    StructField('Explained by: Generosity', DoubleType(), True),
    StructField('Explained by: Perceptions of corruption', DoubleType(), True),
    StructField('Dystopia + residual', DoubleType(), True)
])

# COMMAND ----------

happiness_report_21 = spark.read.option("header", "true").schema(happiness_report_21_schema).csv("dbfs:/FileStore/big-data-processing-nov-2023/world_happiness_report_2021.csv")

# COMMAND ----------

happiness_report_21.printSchema()

# COMMAND ----------

world_h_repo = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/FileStore/big-data-processing-nov-2023/world_happiness_report.csv")

# COMMAND ----------

world_h_repo.printSchema()

# COMMAND ----------

display(world_h_repo)

# COMMAND ----------

# MAGIC %md
# MAGIC # Question 1: What is the “happiest” country in 2021 according to the data? (consider that the “Ladder score” column means the greater the number, the happier the country is)
# MAGIC #### Pregunta 1 ¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score” mayor número más feliz es el país)

# COMMAND ----------

from pyspark.sql.functions import col, max, avg, first, dense_rank, rank, row_number

# COMMAND ----------

max_ladder_score = happiness_report_21.select(col("Country name"), col("Ladder score").alias("Highest Ladder Score")) \
    .orderBy(col('Ladder score').desc()) \
    .limit(1)

# COMMAND ----------

display(max_ladder_score)

# COMMAND ----------

# MAGIC %md
# MAGIC # Question 2: What is the “happiest” country in 2021 by continent according to the data?
# MAGIC #### Pregunta 2 ¿Cuál es el país más “feliz” del 2021 por continente según la data?

# COMMAND ----------

country_region_21 = happiness_report_21.select(col("Regional indicator").alias("Region"), col("Country name").alias("Country"), col("Ladder score").alias("Max Ladder score")) \
    .orderBy(col('Ladder score').desc()) 

best_country_region = country_region_21.dropDuplicates(["Region"])\
    .orderBy(col('Ladder score').desc())

# COMMAND ----------

display(best_country_region)
