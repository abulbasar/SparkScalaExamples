package com.example.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import java.text.SimpleDateFormat
import java.nio.ByteBuffer
import scala.reflect.ClassTag

case class StockType(date: java.sql.Date, open: Double, high: Double, low: Double, close: Double, volume: Double, adjclose: Double, symbol: String) {

  def toBytes = StockType.toBytes(_)
  
  implicit def dateToString(date:java.sql.Date) = date.toString()

  def toPut = {

    val cf = toBytes("info")
    new Put(toBytes(date.toString() + " " + symbol))
      .addColumn(cf, toBytes("date"), Bytes.toBytes(date))
      .addColumn(cf, toBytes("open"), Bytes.toBytes(open))
      .addColumn(cf, toBytes("close"), Bytes.toBytes(close))
      .addColumn(cf, toBytes("high"), Bytes.toBytes(high))
      .addColumn(cf, toBytes("low"), Bytes.toBytes(low))
      .addColumn(cf, toBytes("adjclose"), Bytes.toBytes(adjclose))
      .addColumn(cf, toBytes("volume"), Bytes.toBytes(volume))
      .addColumn(cf, toBytes("symbol"), Bytes.toBytes(symbol))
  }
}

object StockType {
 
  def toBytes(name: String) = Bytes.toBytes(name)

  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  
  implicit def bytebufferToDouble(bytes: ByteBuffer) = bytes.getDouble
  implicit def bytebufferToString(bytes: ByteBuffer) = new String(bytes.array())
  implicit def bytebufferToDate(bytes: ByteBuffer) = new java.sql.Date(sdf.parse(bytes).getTime)
  
  def fromBytes(result: Result, cf: String, identifier: String) = {
    val buffer = ByteBuffer.wrap(result.getValue(toBytes(cf), toBytes(identifier)))
    buffer
  }

  def apply(result: Result): StockType = {

    val date = fromBytes(result, "info", "date")
    val open = fromBytes(result, "info", "open")
    val high = fromBytes(result, "info", "high")
    val low = fromBytes(result, "info", "low")
    val close = fromBytes(result, "info", "close")
    val adjClose = fromBytes(result, "info", "adjclose")
    val volume = fromBytes(result, "info", "volume")
    val symbol = fromBytes(result, "info", "symbol")

    StockType(date, open, high, low, close, volume, adjClose, symbol)
  }

}