package com.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object StockDownloader {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local")

    val spark = SparkSession.builder().config(conf).appName(getClass.getName).getOrCreate()

    import spark.implicits._

    val symbols = Seq("a", "b", "c", "d", "e", "f", "g", "h")
                  .toDF("value").repartition(10)

    val prices = symbols.mapPartitions(symbolsWithinPartitions => {
      val result = ArrayBuffer.empty[String]
      symbolsWithinPartitions.foreach(symbol => {
        val path = "https://api.blockchain.info/charts/market-price?format=csv&timespan=all"
        val lines = Source.fromURL(path).mkString.split("\n")
        result.appendAll(lines)
      })
      result.toIterator
    })
    prices.cache()

    println("Number of records", prices.count())

    prices.coalesce(1).write.text("prices")

    spark.stop()

  }

}