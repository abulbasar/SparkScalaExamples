package com.example.hbase

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result


object HBaseReader {

  def main(args: Array[String]) {
    
    val tableName = "stocks"
    println(s"Table name: $tableName")
    
    val spark = AppConnectionFactory.openSparkSession
    
    val hbaseConf = AppConnectionFactory.getHBaseConf
    
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "stocks")
    hbaseConf.set("hbase.zookeeper.quorum", "localhost:2181")
    hbaseConf.set("timeout", "120000")
    
    import spark.implicits._
    
    
    val hbaseStocksRdd = spark.sparkContext.newAPIHadoopRDD(hbaseConf
                , classOf[TableInputFormat]
                , classOf[ImmutableBytesWritable]
                , classOf[Result])
                
    val stocks = hbaseStocksRdd.map(pair => StockType(pair._2)).toDS
    stocks.show()

  }
}