package com.tudux.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType, IntegerType}

object PopulationToPostgres extends App {

  //create a spark session
  val spark = SparkSession.builder()
    .appName("Dataframe Population")
    .config("spark.master", "local")
    .getOrCreate()

  //{"year":1900,"age":60,"sex":1,"people":916571}
  val populationSchema = StructType(Array(
    StructField("year", IntegerType),
    StructField("age", IntegerType),
    StructField("sex", IntegerType),
    StructField("people", LongType)
  ))

  val populationDF = spark.read
    .format("json")
    .schema(populationSchema)
    .load("src/main/resources/data/population.json")

  populationDF.show()


}
