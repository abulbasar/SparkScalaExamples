package com.example.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DStreamApp {

  /*
  Start socket source
  $ nc -nlk 9999
  
  */
  def main(args:Array[String]) {

    val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local[*]")

    val sc = new SparkContext(conf)

    val checkpointDir = "/tmp/streaming/checkpoint/" + sc.applicationId + "/"
    val rawStorage = "/tmp/streaming/" + sc.applicationId + "/raw"

    def createSSC() = {
      val ssc =  new StreamingContext(sc, Seconds(5))
      ssc.checkpoint(checkpointDir)
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDir, createSSC)

    val raw = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)

    raw.saveAsTextFiles(rawStorage , ".data")

    //raw.print()

    /*
    raw.map(line => {
      line.toUpperCase
    }).print(10)
    */


    raw.mapPartitions((batch: Iterator[String]) => {
    
      val partitionOutput = batch.map((record: String) => {
        record.toUpperCase
      })
      partitionOutput
    
    }).print()
    

    ssc.start()
    ssc.awaitTermination()

  }
}
