package com.example.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}


case  class Stock(date:java.sql.Timestamp, open:Double, high:Double
                  , low:Double, close:Double, volume:Double
                  , adjclose:Double, symbol:String)


object DatasetExample {

  val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local")

  val spark:SparkSession = SparkSession.builder()
    .config(conf).appName(getClass.getName).getOrCreate()

  val sc = spark.sparkContext


  def findStockRecommendationRow(stocks: Dataset[Row]) = {
    // Not optimized ...
    // you have to write code to verify that the stocks dataframe
    // contains data as per your expectation
  }

  def findStockRecommendation(stocks: Dataset[Stock]) = {
    // Optimized ...
    // You do not have to write any custom code to verify the schema
    // Schema is already defined as case class
  }

  def convertRddToDataset(inputPath:String):Dataset[Stock] = {
    val rdd = sc.textFile(inputPath)

    val rddStocks:RDD[Stock] = rdd
      .filter(!_.startsWith("date"))
      .map(line => {
        val tokens = line.split(",")
        Stock(
          null
          , tokens(1).toDouble
          , tokens(2).toDouble
          , tokens(3).toDouble
          , tokens(4).toDouble
          , tokens(5).toDouble
          , tokens(6).toDouble
          , tokens(7)
        )
      })
    import spark.implicits._
    rddStocks.toDS()
  }


  def convertTextToDataset(inputPath:String):Dataset[Stock] = {
    import spark.implicits._
    val ds:Dataset[String] = spark.read.text(inputPath).as[String]

    ds
      .filter(("value not like 'date%'"))
      .map(line => {
        val tokens = line.split(",")
        Stock(
          null
          , tokens(1).toDouble
          , tokens(2).toDouble
          , tokens(3).toDouble
          , tokens(4).toDouble
          , tokens(5).toDouble
          , tokens(6).toDouble
          , tokens(7)
        )
      })
  }

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)

    import spark.implicits._

    val options = Map("header" -> "true", "inferSchema" -> "true")
    val stocks:Dataset[Row] = spark.read.options(options).csv(inputPath)

    findStockRecommendation(stocks.as[Stock])

  }

}

