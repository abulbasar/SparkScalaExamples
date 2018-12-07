package com.example

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* Before running this application, start the socket streaming source by
* $ nc -nlk 9999
*
* */

object StreamAppDirectoryWatch {
  
   
  def main(args:Array[String]) {
    
    val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local[*]")
    
    val sc = new SparkContext(conf)

    val timestamp = new java.util.Date().getTime
    
    val checkpointDir = s"spark-checkpoint $timestamp"

    val rawStorage = "raw-storage"
    
    def createSSC() = {
      val ssc =  new StreamingContext(sc, Seconds(5))
      ssc.checkpoint(checkpointDir)
      ssc
    }
    
    val ssc = StreamingContext.getOrCreate(checkpointDir, createSSC)
    
    val raw = ssc.textFileStream("/tmp/input")
    
    raw.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}
