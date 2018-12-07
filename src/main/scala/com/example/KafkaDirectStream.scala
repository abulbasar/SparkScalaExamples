package main.scala

package com.example

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/*
 * Set the project JDK version to 1.8 for lambda expressions to work.
 */

object KafkaDirectStream {

  var spark:SparkSession = _
  var sc:SparkContext = _

  def main(args: Array[String]): Unit = {

    val appName = getClass.getName
    val conf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", "local[*]")

    spark = SparkSession.builder().config(conf).getOrCreate()
    sc = spark.sparkContext

    val topics = Seq("T1").toSet

    val ssc = new StreamingContext(sc, Seconds(3))

    val kafkaParams = Map[String, String](
        "bootstrap.servers" -> "localhost:9092"
      , "group.id" -> "spark_streaming"
      , "auto.offset.reset"-> "largest"
    )


    val stream = KafkaUtils.createDirectStream[String, String
          , StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val values = stream.map(_._2)
    values.print()

    
    ssc.start()
    ssc.awaitTermination()
  }
}
