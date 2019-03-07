package com.example.streaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/*

/usr/lib/spark-2.2.3-bin-hadoop2.7/bin/spark-submit \
--verbose \
--class com.example.streaming.kafka.KafkaDirectStream \
--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.3 \
~/SparkScalaExamples/target/SparkScalaExamples_0.1.jar


* */

case class Message(key:String,
                    value: String,
                    offset: Long,
                    topic:String,
                    partition: Int,
                    keySize: Int,
                    valueSize: Long,
                    timestamp: Long,
                   checksum: Long
                  )

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

    val topicName = "demo"

    val topics = Seq(topicName).toSet

    val ssc = new StreamingContext(sc, Seconds(3))

    val kafkaParams = Map(
        "bootstrap.servers" -> "localhost:9092"
      , "group.id" -> "spark_streaming"
      , "auto.offset.reset"-> "latest"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "enable.auto.commit" -> "false"
    )


    val stream: DStream[ConsumerRecord[String, String]] =
          KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
          )

    val messages: DStream[Message] = stream.map(r => Message(
                      r.key()
                      , r.value()
                      , r.offset()
                      , r.topic()
                      , r.partition()
                      , r.serializedKeySize()
                      , r.serializedValueSize()
                      ,r.timestamp()
                      , r.checksum()))

    messages.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
