package com.tudux.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, expr, lit, year}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

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
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  firstDF.show()

  val dfWithProvidedSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  dfWithProvidedSchema.show()
  dfWithProvidedSchema.take(10).foreach(println)


  //Expressions and Select
  val dfWithKmsPerLiter = dfWithProvidedSchema.select(
    col("Name"),
    col("Miles_per_Gallon"),
    expr("Miles_per_Gallon * 0.425144").as("KM_per_Liter"),
    col("Year"),
    col("Origin")
  )

  dfWithKmsPerLiter.show(20)

  val dfWithFilter = dfWithKmsPerLiter
    .filter(col("KM_per_Liter") < 8)
    .filter(col("Origin") === "USA")
    .filter(year(col("Year")).geq(lit(1980)))
  dfWithFilter.show(50)


}
