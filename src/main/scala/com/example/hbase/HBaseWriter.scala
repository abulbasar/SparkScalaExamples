package com.example.hbase

import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Table

import scala.collection.JavaConverters._

object HBaseWriter {
  def main(args: Array[String]) {
    val inPath = args(0)
    val spark = AppConnectionFactory.openSparkSession
    lazy val conn = AppConnectionFactory.openHBaseConnection
    
    import spark.implicits._

    val stocks = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv(inPath)
      .withColumn("date", expr("cast(date as date)"))
      .as[StockType]
    
    stocks.foreachPartition{batch =>
      val tableName = TableName.valueOf("stocks")
      val table = conn.getTable(tableName)
      val puts = batch.map(_.toPut).toList.asJava
      table.put(puts)
    }

    stocks.show()

  }
}