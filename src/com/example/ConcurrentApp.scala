package com.example

import java.util.concurrent.Executors

import scala.collection.Seq
import scala.concurrent._
import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession


object ConcurrentApp {
  
  
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local[*]")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    val pool = Executors.newFixedThreadPool(5)

    implicit val xc = ExecutionContext.fromExecutorService(pool)

    val df = spark.range(100).toDF("id")

    val tasks = Seq(
      taskA(df, "out1"),
      taskB(df, "out2"))
      
    Await.result(Future.sequence(tasks), Duration(1, MINUTES))
    spark.close()
    pool.shutdown()
  }

  def taskA(df: Dataset[Row], outPath: String)(implicit xc: ExecutionContext) = Future {
    df.distinct.write.mode(SaveMode.Overwrite).save(outPath)
  }

  def taskB(df: Dataset[Row], outPath: String)(implicit xc: ExecutionContext) = Future {
    df.distinct.write.mode(SaveMode.Overwrite).save(outPath)
  }
}
