package com.example.streaming.sinks

import org.apache.spark.sql.ForeachWriter
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes


case class HBaseRecordType(value:Double, outlier:Boolean)


/*
To query the hbase table using hive, create the following table.

CREATE EXTERNAL TABLE outliers(
	key BIGINT,
	value DOUBLE,
	outlier BOOLEAN)
ROW format serde 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES('hbase.columns.mapping' = ':key,info:value,info:outlier')
TBLPROPERTIES (
'hbase.table.name' = 'outliers',
'hbase.table.default.storage.type' = 'binary');


*/

class HBaseSink extends ForeachWriter[HBaseRecordType] {

  def getHBaseConf = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost:2181")
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf
  }
  lazy val hbaseConnection = ConnectionFactory.createConnection(getHBaseConf)
  lazy val table = hbaseConnection.getTable(TableName.valueOf("outliers"))

  def open(partitionId: Long, version: Long): Boolean = {

    true
  }

  def toBytes(value:String) = Bytes.toBytes(value)
  def toBytes(value:Double) = Bytes.toBytes(value)
  def toBytes(value:Boolean) = Bytes.toBytes(value)


  def process(rec: HBaseRecordType) {

    print(rec)

    val now = new java.util.Date()

    val put: Put = new Put(toBytes(now.toInstant().getNano))
    put.addColumn(toBytes("info"), toBytes("value"), toBytes(rec.value))
    put.addColumn(toBytes("info"), toBytes("outlier"), toBytes(rec.outlier))
    table.put(put)

  }

  def close(throwable: Throwable) {

    hbaseConnection.close()
  }

}
