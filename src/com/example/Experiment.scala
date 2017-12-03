package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType

object Experiment {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local[*]")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
      
    import spark.implicits._
    

  }
}