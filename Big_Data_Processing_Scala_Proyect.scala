// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, min, max}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// COMMAND ----------

//Create a Dataframe from a .csv file

val dfHappinessReport21 = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.csv("dbfs:/FileStore/big-data-processing-nov-2023/world_happiness_report_2021.csv")

display(dfHappinessReport21)

// COMMAND ----------

//Create a Dataframe from a .csv file

val dfWorldHapRep = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.csv("dbfs:/FileStore/big-data-processing-nov-2023/world_happiness_report.csv")

display(dfWorldHapRep)

// COMMAND ----------

// MAGIC %md
// MAGIC # Question 1: What is the “happiest” country in 2021 according to the data? (consider that the “Ladder score” column means the greater the number, the happier the country is)
// MAGIC #### Pregunta 1 ¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score” mayor número más feliz es el país)

// COMMAND ----------

// MAGIC %md
// MAGIC Question 1 - Solution 1

// COMMAND ----------

//This function sorts the Ladder Score descendingly and returns only one record

val theHappiest21 = dfHappinessReport21.select(col("Country name"), col("Ladder score").as("Highest Ladder Score"))
.orderBy(col("Ladder score").desc)
.limit(1)

display(theHappiest21)

// COMMAND ----------

// MAGIC %md
// MAGIC Question 1 - Solution 2

// COMMAND ----------

//This function groups the countries by the Country name column, adds a new column and in it shows the maximum value of each country for the Ladder Score value and finally returns the first record.

val theHappiest21V2 = dfHappinessReport21.groupBy(col("Country name"))
  .agg(max(col("Ladder score")).as("Highest Ladder Score"))
  .orderBy(col("Highest Ladder Score").desc)
  .limit(1)

theHappiest21V2.show

// COMMAND ----------

// MAGIC %md
// MAGIC # Question 2: What is the “happiest” country in 2021 by continent according to the data?
// MAGIC #### Pregunta 2 ¿Cuál es el país más “feliz” del 2021 por continente según la data?

// COMMAND ----------

// MAGIC %md
// MAGIC Question 2 - Solution 1

// COMMAND ----------

val countryRegion21 = dfHappinessReport21.select(col("Regional indicator"), col("Country name"), col("Ladder score"))
  .dropDuplicates("Regional indicator")
  .orderBy(col("Ladder score").desc)


display(countryRegion21)

// COMMAND ----------

// MAGIC %md
// MAGIC # Question 3: Which country has ranked first most times in all years?
// MAGIC #### Pregunta 3: ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

// COMMAND ----------

// MAGIC %md
// MAGIC Question 3 - Solution 1

// COMMAND ----------

//Create a dataframe with the record of the 2021 winner

val winner2021 = Seq(
      ("Finland", 2021, 7.842),
      )

val dfWinner2021 = spark.createDataFrame(winner2021)
    .toDF("Country", "year", "MaxLifeLeader")

dfWinner2021.show()

// COMMAND ----------

//Create a dataframe with the necessary columns

val dfNewWorldHapRep = dfWorldHapRep.select(col("Country name"), 
  col("year"), 
  col("Life Ladder") as ("MaxLifeLeader"))

dfNewWorldHapRep.show()

// COMMAND ----------

//I create a new dataframe with the union of the previous two

val dfAllYearsWH = dfNewWorldHapRep.union(dfWinner2021)

dfAllYearsWH.show()


// COMMAND ----------

// Checking the union
val filtered2021DF = dfAllYearsWH.filter($"year" >= 2021)


filteredDF.show(false)

// COMMAND ----------

// Checking the union
val filtered2017DF = dfAllYearsWH.filter($"year" >= 2017)

filtered2017DF.show(false)

// COMMAND ----------

// Define partition window by year and sort by Life Ladder
val windowRank = Window.partitionBy("year").orderBy(desc("MaxLifeLeader"))

// Add a ranking column using row_number()
val rankeddf2 = dfAllYearsWH.withColumn("Rank", row_number().over(windowRank))

// This function answer the question "Which country has ranked first most times in all years?""
val topCountry = rankeddf2
  .filter(col("Rank") === 1)// This line filters the dataframe rankeddf2 and it takes only registers with Rank 1
  .groupBy("Country name")
  .count().as("How many")// This line count how much times a country had won
  .orderBy(desc("count"))// This line order the count in descending order

topCountry.show()

// COMMAND ----------

//Winners

val dfwinners = topCountry.head(2)
display(dfwinners)

// COMMAND ----------

import org.apache.spark.sql.functions._

// Filter the rows where the Rank is 1 and group by "Country name"
val topCountryStats = rankeddf2
  .filter(col("Rank") === 1)
  .groupBy("Country name")
  .agg(
    count("Rank").as("Number of times first"),
    collect_list("year").as("Years first")
  )
  .orderBy(desc("Number of times first"))
  

// Show the result
display(topCountryStats)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 4: What Happiness ranking does the country with the highest GDP in 2020 have?
// MAGIC #### Pregunta 4: ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?

// COMMAND ----------

// MAGIC %md
// MAGIC Question 4 - Solution 1

// COMMAND ----------

// Define partition windows by yea r and sort by Log GDP per capita and Life Ladder
val windowRank2 = Window.partitionBy("year").orderBy(desc("Log GDP per capita"))
val windowRank3 = Window.partitionBy("year").orderBy(desc("Life Ladder"))

// Add a ranking column using row_number()
val rankedGDPdf = dfWorldHapRep
.withColumn("RankGDP", row_number().over(windowRank2))
.withColumn("RankLifeLadder", row_number().over(windowRank3))

// This function answer the question "What Happiness ranking does the country with the highest GDP in 2020 have?""
val topGDPCountry2020 = rankedGDPdf
  .filter(col("year") === 2020)
  .filter(col("RankGDP") === 1)
  .select("Country name", "Life Ladder", "RankLifeLadder")
  .first()  // Obtener el primer registro, que es el país con el mayor GDP en 2020

println(s"The country with the highest GDP in 2020 was ${topGDPCountry2020.getAs[String]("Country name")} with ${topGDPCountry2020.getAs[Double]("RankLifeLadder")}th position in the happiness ranking and ${topGDPCountry2020.getAs[Double]("Life Ladder")} on the Life Ladder score")

// COMMAND ----------

// MAGIC %md
// MAGIC Question 4 - Solucion 1.2

// COMMAND ----------

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

// Define the schema for your Row
val schema = StructType(Seq(
  StructField("Country name", StringType, nullable = true),
  StructField("Life Ladder", DoubleType, nullable = true),
  StructField("RankLifeLadder", IntegerType, nullable = true)
))

// Create a Spark session
//val spark = SparkSession.builder().appName("example").getOrCreate()

// Create an RDD of Row
val topGDPCountry2020RDD = spark.sparkContext.parallelize(Seq(topGDPCountry2020))

// Create a DataFrame from the RDD with the defined schema
val topGDPCountry2020DF = spark.createDataFrame(topGDPCountry2020RDD, schema)

// Display the DataFrame
display(topGDPCountry2020DF)

// COMMAND ----------

// MAGIC %md
// MAGIC Question 4 - Solution 2

// COMMAND ----------

// Define partition windows by year and sort by Log GDP per capita and Life Ladder
val windowRank2 = Window.partitionBy("year").orderBy(desc("Log GDP per capita"))
val windowRank3 = Window.partitionBy("year").orderBy(desc("Life Ladder"))


// Add a ranking column using row_number()
val rankedGDPdf = dfWorldHapRep
.withColumn("RankGDP", row_number().over(windowRank2))
.withColumn("RankLifeLadder", row_number().over(windowRank3))

// This function answer the question "What Happiness ranking does the country with the highest GDP in 2020 have?""
val topGDPCountry2020V2 = rankedGDPdf
  .filter(col("year") === 2020)
  .filter(col("RankGDP") === 1)
  .select("Country name", "Life Ladder", "RankLifeLadder")
  .limit(1)  // Obtener el primer registro, que es el país con el mayor GDP en 2020

display(topGDPCountry2020V2)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 5: In percentage terms, how much has the Worldwide average GDP changed between 2020 and 2021? Did it increase or decrease?
// MAGIC #### Pregunta 5: ¿En que porcentaje ha variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?

// COMMAND ----------

//Create a dataframe with 2020 data only

val dfWorldHapRep2020 = dfWorldHapRep.select("*")
  .filter(col("year")===2020)

dfWorldHapRep2020.show

// COMMAND ----------

//Delete entire records that contain nulls in the GDP column

val dfWorldHapRep21NoNulls = dfHappinessReport21.na.drop("all", Seq("Logged GDP per capita"))
val dfWorldHapRep20NoNulls = dfWorldHapRep2020.na.drop("all", Seq("Log GDP per capita"))

// COMMAND ----------

//Check original DF records number

dfHappinessReport21.count()

// COMMAND ----------

//Check not null DF records number
dfWorldHapRep21NoNulls.count()

// COMMAND ----------

//Check original DF records number
dfWorldHapRep2020.count()

// COMMAND ----------

//Check not null DF records number
dfWorldHapRep20NoNulls.count()

// COMMAND ----------

// Calculate the average of the "Log GDP per capita" column
val averageGDP2020 = dfWorldHapRep20NoNulls.select(avg(col("Log GDP per capita")).as("average 2020")).collect()(0)(0).asInstanceOf[Double]

// Imprime el resultado
println(s"The 2020 average Log GDP per capita is: $averageGDP2020")


// COMMAND ----------

// Calculate the average of the "Logged GDP per capita" column
val averageGDP2021 = dfWorldHapRep21NoNulls.select(avg(col("Logged GDP per capita")).as("average 2021")).collect()(0)(0).asInstanceOf[Double]

// Imprime el resultado
println(s"The 2021 average Log GDP per capita is: $averageGDP2021")
//averageGDP2021.show

// COMMAND ----------

// MAGIC %md
// MAGIC Question 5 - Solution 1

// COMMAND ----------

val percentage2020vs2021 = ((averageGDP2021 - averageGDP2020) / averageGDP2020) * 100

println(s"From 2020 to 2021 the average Log GDP per capita has changed percentageally: $percentage2020vs2021 %")

if(percentage2020vs2021 > 0) println("The average Log GDP has increased")
else if(percentage2020vs2021 < 0) println("The average Log GDP has decreased")
else println("There has been no change")
