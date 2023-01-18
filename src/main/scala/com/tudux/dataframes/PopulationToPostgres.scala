package com.tudux.dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object PopulationToPostgres extends App {

  val log: Logger = LoggerFactory.getLogger(PopulationToPostgres.getClass.getName)
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

  val populationByYearDf = populationDF.filter(col("year") === 1970)
    .select(sum(col("people")))
  populationByYearDf.show()

  val populationByGroupYearDf = populationDF
    .groupBy(col("year"))
    .agg(
      sum(col("people"))
    ).orderBy(col("year"))

  populationByYearDf.show()
  populationByGroupYearDf.show()


  val result = Try({

    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost:5432/rtjvm"
    val user = "docker"
    val password = "docker"

    populationByGroupYearDf
      .write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", "PEOPLE_BY_YEAR")
      .option("driver", driver)
      .save()
  })

  result match {
    case Success(_) => log.info("table updated")
    case Failure(problem) => log.error(s"table update failed with $problem")

  }



}

