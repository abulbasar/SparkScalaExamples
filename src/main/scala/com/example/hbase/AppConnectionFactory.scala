package com.example.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.hbase.mapreduce.TableInputFormat


object AppConnectionFactory {

  def getHBaseConf = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost:2181")
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf
  }

  def openHBaseConnection = ConnectionFactory.createConnection(getHBaseConf)

  val sparkConf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local")

  def openSparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

}