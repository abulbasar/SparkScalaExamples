package com.example.streaming.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/*

/usr/lib/spark-2.2.3-bin-hadoop2.7/bin/spark-submit \
--verbose \
--class com.example.streaming.kafka.KafkaStructuredStream \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.3 \
~/SparkScalaExamples/target/SparkScalaExamples_0.1.jar

*/

object KafkaStructuredStream {

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

    val kafkaParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092"
      , "group.id" -> "spark_streaming"
      , "auto.offset.reset"-> "latest"
      , "key.deserializer" -> classOf[StringDeserializer].getName
      , "value.deserializer" -> classOf[StringDeserializer].getName
      , "enable.auto.commit" -> "false"
    )

    val messages = spark.readStream
      .format("kafka")
      .options(kafkaParams.toMap)
      .option("subscribe", topicName)
      .load()


    messages.printSchema()

    messages
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset")
      .writeStream
      .format("console")
      .start()

    spark.streams.awaitAnyTermination()
    spark.close()

  }
}
