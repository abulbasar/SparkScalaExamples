package com.example

import com.example.WordCount.getClass
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local")

    val sc = new SparkContext(conf)

    val processed = sc.longAccumulator("processed")
    val rdd = sc.parallelize(0 until 100, 4)


    rdd.map{v =>
      processed.add(1)
      Thread.sleep(500)
      (v, v * 2)
    }.collect().foreach(println)

    val count = processed.value

    println(s"Total processed: $count")

    println("Process is complete. Please press enter to terminate.")
    System.in.read()

  }

}
