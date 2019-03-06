package com.example.streaming.kafka

import com.example.PropertiesLoader
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterAnalyzer {

  def main(args:Array[String]) {

    val conf = new SparkConf()
      .setAppName("DirectKafkaWordCount")
      .setIfMissing("spark.master", "local")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = PropertiesLoader.loadAsMap("kafka.properties")
    val appParams = PropertiesLoader.loadAsMap("app.properties")
    val topic = appParams.get("topic").get
    val topics = Array(topic).toSet

    val toTags = functions.udf{(msg: String) =>
        val tokens = msg.toLowerCase.split("\\s+").filter(_.startsWith("#"))
        tokens
    }



    val messages = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //messages.map(_._2).print()

    messages.map(_._2).foreachRDD{rdd =>

      if(!rdd.isEmpty()){

        try{
          spark
          .read
          .json(rdd)
          .select("text")
          .select(toTags(functions.col("text")).alias("tags"))
          .select(functions.explode(functions.col("tags")).alias("tag"))
          .groupBy("tag")
          .count
          .orderBy(functions.desc("count"))
          .show(5)
        }catch{
          case ex:Exception => ex.printStackTrace()
        }

      }

    }

    //.saveAsTextFiles(topic, "txt")

    /*var offsetRanges = Array[OffsetRange]()
    messages.transform{ rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      rdd
    }.map(_._2).print()*/

    ssc.start()
    ssc.awaitTermination()
  }
}
