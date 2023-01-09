package com.tudux.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFrameBasics extends App {

  //create a spark session
  val spark = SparkSession.builder()
    .appName("Dataframe Basics")
    .config("spark.master", "local")
    .getOrCreate()

  //reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/cars.json")

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  firstDF.show()

  val dfWithProvidedSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  dfWithProvidedSchema.show()
  dfWithProvidedSchema.take(10).foreach(println)

}
