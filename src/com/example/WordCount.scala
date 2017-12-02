package com.example

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object WordCount {
  
  def main(args: Array[String]): Unit = {
    
    if(args.size == 0){
      println("Usage: WordCount <input path>")
      System.exit(0)
    }
    
    val path = args(0)
    print(s"Input path: $path")
    
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local")
      
    val sc = new SparkContext(conf)
    
    sc.textFile(path)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.length > 0)
      .map((_, 1))
      .reduceByKey(_+_)
      .collect
      .foreach(println)
  }
}
