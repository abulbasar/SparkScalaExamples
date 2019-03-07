package com.example.streaming

import com.example.helper.CassandraSink
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredStreamFileSourceApp {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local[*]")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    /*
     spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent) {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent) {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent) {
        println("Query made progress: " + queryProgress.progress)
      }
    })
    */

    //Source
    val stream = spark
      .readStream
      .format("csv")
      .text("/tmp/source")

    print("Storage level: ", stream.storageLevel)

    //Sink 1
    stream
      .writeStream
      .trigger(Trigger.ProcessingTime(0))
      .outputMode("append")
      .format("console")
      .start()

    //Sink 2
    stream
      .writeStream
      .outputMode("append")
      .format("csv")
      .option("path", "output/")
      .option("checkpointLocation", "checkpoint")
      .start()

    val cassandraSink = new CassandraSink("demo"
      , "localhost:9042")

    stream
      .writeStream
      .foreach(cassandraSink)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .start()

    spark.streams.awaitAnyTermination()

  }
}
