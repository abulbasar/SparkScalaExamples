package com.example.projects.retail

import com.example.hbase.AppConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
/*

/usr/lib/spark-2.2.3-bin-hadoop2.7/bin/spark-submit \
--verbose \
--class com.example.projects.retail.KafkaDirectStream \
--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.3 \
target/SparkScalaExamples_0.1-jar-with-dependencies.jar


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

  def getHBaseConf = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost:2181")
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf
  }

  def openHBaseConnection = ConnectionFactory.createConnection(getHBaseConf)

  def main(args: Array[String]): Unit = {

    val appName = getClass.getName
    val conf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", "local[*]")

    spark = SparkSession.builder().config(conf).getOrCreate()
    sc = spark.sparkContext

    val topicName = "retail"

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
                      , r.timestamp()
                      , r.checksum()))

    val parsed:DStream[SalesRecord] =  messages.map(_.value).map(SalesRecord.parse)

    parsed.print()

    lazy val conn = AppConnectionFactory.openHBaseConnection

    parsed.foreachRDD((rdd:RDD[SalesRecord]) => {
      rdd.foreachPartition{batch =>
        val tableName = TableName.valueOf("retail")
        val table = conn.getTable(tableName)
        val puts = batch.map(_.toPut).toList.asJava
        table.put(puts)
      }

    })
    ssc.start()
    ssc.awaitTermination()
  }
}
