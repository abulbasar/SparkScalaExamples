package com.example.streaming

import com.example.streaming.sinks.CassandraSink
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructField, StructType, _}

case class MyStock2(
                    date:String,
                    open:Double,
                    high: Double,
                    low: Double,
                    close: Double,
                    volume: Double,
                    adjclose: Double,
                    symbol:String
                  )


object StructuredStreamFileSourceApp {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(getClass.getName).setIfMissing("spark.master", "local[*]")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()


    import spark.implicits._

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


    val schema = new StructType()
    schema.add(new StructField("date", StringType))
    schema.add(new StructField("open", DoubleType))
    schema.add(new StructField("high", DoubleType))
    schema.add(new StructField("low", DoubleType))
    schema.add(new StructField("close", DoubleType))
    schema.add(new StructField("adjclose", DoubleType))
    schema.add(new StructField("volume", DoubleType))
    schema.add(new StructField("symbol", StringType))


    //Source
    val stream:DataFrame = spark
      .readStream
      .format("csv")
      //.schema(schema)
      .text("/tmp/source")

    stream.printSchema()

    println(s"Stream, isStreaming: ${stream.isStreaming}")

    print("Storage level: ", stream.storageLevel)

    val parsed = stream.as[String].map(line => {
      val tokens = line.split(",")
      MyStock2(
        tokens(0),
        tokens(1).toDouble,
        tokens(2).toDouble,
        tokens(3).toDouble,
        tokens(4).toDouble,
        tokens(5).toDouble,
        tokens(6).toDouble,
        tokens(7)
      )
    })

    //Sink 1: console
    parsed
      .writeStream
      .trigger(Trigger.ProcessingTime(0))
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .option("numRows", 10)
      .start()

    //Sink 2: filesystem
    stream
      .writeStream
      .outputMode("append")
      .format("csv")
      .option("path", "/tmp/output/")
      .option("checkpointLocation", "checkpoint")
      .start()

    // Sink 3: cassandra
    /*
    stream
      .writeStream
      .foreach(new CassandraSink("demo", "localhost:9042"))
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .start()

    */

    spark.streams.awaitAnyTermination()

  }
}
