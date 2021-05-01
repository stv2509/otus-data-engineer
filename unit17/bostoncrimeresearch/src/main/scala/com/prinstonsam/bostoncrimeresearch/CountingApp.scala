package com.prinstonsam.bostoncrimeresearch

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object CountingLocalApp extends App {
  val (inputFile_crime, inputFile_offense, outputFile) = (args(0), args(1), args(2))
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("bigdata-competition")
    .getOrCreate()

  Runner.run(spark, inputFile_crime, inputFile_offense, outputFile)
}

object Runner {
  implicit def offenceCodesEncoder: Encoder[OffenseCodes] = Encoders.product

  implicit def crimeEncoder: Encoder[Crime] = Encoders.product

  implicit def topThreeCrimesEncoder: Encoder[TopThreeCrimes] = Encoders.product

  implicit def ResultEncoder: Encoder[Result] = Encoders.product

  def run(spark: SparkSession, inputFile_crime: String, inputFile_offence: String, outputFile: String): Unit = {


    val crimes = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputFile_crime)
      .map(row => Crime(
        row.getAs[String]("INCIDENT_NUMBER"),
        row.getAs[Int]("OFFENSE_CODE").toString,
        row.getAs[String]("DISTRICT"),
        row.getAs[Int]("YEAR"),
        row.getAs[Int]("MONTH"),
        row.getAs[Double]("Lat"),
        row.getAs[Double]("Long")))
      .filter(col("district").isNotNull)
      .cache()

    import org.apache.spark.sql.functions._


    val agg1 = crimes.groupBy("district", "month")
      .agg(count("month"), avg("lat"), avg("lon"))
      .groupBy("district")
      .agg(
        avg("count(month)").name("count_months"),
        avg("avg(lat)").name("avg_lat"),
        avg("avg(lon)").name("avg_lon"))

    val offenseCodes = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputFile_offence)
      .map(row => OffenseCodes(row.getAs[Int]("CODE"), row.getAs[String]("NAME").split(" - ")(0)))

    spark.sparkContext.broadcast(offenseCodes)

    val agg2 = crimes
      .join(offenseCodes, crimes("offenseCode") === offenseCodes("name"))
      .groupBy("district", "code")
      .agg(count("code"))
      .orderBy(col("district").desc, col("count(code)").desc)
      .withColumn("row_num",
        row_number()
          .over(Window.partitionBy("district").orderBy("district")))
      .filter(col("row_num") < 4)
      .map(row => TopThreeCrimes(row.getAs[String]("district"), row.getAs[String]("code")))
      .groupBy("district")
      .agg(concat_ws(", ", collect_list(col("code"))).name("offense_names"))


    val commonAgg = agg1
      .join(agg2, "district")
      .map(row =>
        Result(
          row.getAs[String]("district"),
          Math.scalb(row.getAs[Double]("count_months"), 2),
          Math.scalb(row.getAs[Double]("avg_lat"), 2),
          Math.scalb(row.getAs[Double]("avg_lon"), 2),
          row.getAs[String]("offense_names")))

    commonAgg.repartition(1).write.parquet(outputFile)
  }
}
