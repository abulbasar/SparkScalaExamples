package com.example.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


case class MyStock(
                  date:String,
                  open:Double,
                  high: Double,
                  low: Double,
                  close: Double,
                  volume: Double,
                  adjclose: Double,
                  symbol:String
                )

object DStreamFileSourceApp {


  def main(args:Array[String]) {

    val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val checkpointDir = "/tmp/streaming/checkpoint/" + sc.applicationId + "/"
    val rawStorage = "/tmp/streaming/" + sc.applicationId + "/"

    def createSSC() = {
      val ssc =  new StreamingContext(sc, Seconds(5))
      ssc.checkpoint(checkpointDir)
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDir, createSSC)

    val raw = ssc.textFileStream("/tmp/input")

    //raw.print()
    /*
    raw.map((line:String) => {
      if(!line.startsWith("date")) {
        val tokens = line.split(",")
        val open = tokens(1).toDouble
        val close = tokens(4).toDouble
        val pct = 100 * (close - open)/open
        (line, pct)
      }
    }).print()

    */

    raw.foreachRDD((rdd:RDD[String]) => {
      val ds = rdd.map((line:String) => {
        val tokens = line.split(",")
        MyStock(
          tokens(0),
          tokens(1).toDouble,
          tokens(2).toDouble,
          tokens(3).toDouble,
          tokens(4).toDouble,
          tokens(5).toDouble,
          tokens(6).toDouble,
          tokens(7)
        )
      }).toDS()

      ds.createOrReplaceTempView("stocks")

      //ds.show()

      sqlContext.sql("select *, 100*(close-open)/open as pct from stocks").show()

    })

    ssc.start()
    ssc.awaitTermination()

  }
}
