package com.example.streaming

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


case  class Stock(date:java.sql.Timestamp, open:Double, high:Double
                  , low:Double, close:Double, volume:Double
                  , adjclose:Double, symbol:String)


object DStreamFileSourceStocksApp {

  def toStock(line:String):Stock = {
    val tokens = line.split(",")
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    Stock(
      new Timestamp(dateFormatter.parse(tokens(0)).getTime)
      , tokens(1).toDouble
      , tokens(2).toDouble
      , tokens(3).toDouble
      , tokens(4).toDouble
      , tokens(5).toDouble
      , tokens(6).toDouble
      , tokens(7)
    )
  }

  def main(args:Array[String]) {

    val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local[*]")


    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext

    val checkpointDir = "/tmp/streaming/checkpoint/" + sc.applicationId + "/"
    val rawStorage = "/tmp/streaming/" + sc.applicationId + "/"

    def createSSC() = {
      val ssc =  new StreamingContext(sc, Seconds(5))
      ssc.checkpoint(checkpointDir)
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDir, createSSC)

    val raw:DStream[String] = ssc.textFileStream("/tmp/input")


    raw.foreachRDD(rdd => {

      if(!rdd.isEmpty()) {

        import spark.implicits._

        val batch: Dataset[Stock] = rdd
          .filter(!_.startsWith("date"))
          .map(toStock)
          .toDS

        batch
          .groupBy("symbol")
          .agg(functions.max("adjclose").alias("max"))
          .show()
      }

    })



    ssc.start()
    ssc.awaitTermination()

  }
}
