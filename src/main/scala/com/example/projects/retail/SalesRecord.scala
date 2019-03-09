package com.example.projects.retail

import java.sql.Date
import java.text.SimpleDateFormat

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

case class SalesRecord (
    invoiceNo: Long
    , stockCode: String
    , description: String
    , quantity: Int
    , invoiceDate: java.sql.Date
    , unitPrice: Double
    , customerId:Long
    , country:String){

  def toBytes(s: String) = Bytes.toBytes(s)

  def toPut = {
    val rowKey = s"$invoiceNo:$stockCode"
    val cf = toBytes("info")
    new Put(toBytes(rowKey))
      .addColumn(cf, toBytes("invoiceNo"), Bytes.toBytes(invoiceNo))
      .addColumn(cf, toBytes("stockCode"), Bytes.toBytes(stockCode))
      .addColumn(cf, toBytes("description"), Bytes.toBytes(description))
      .addColumn(cf, toBytes("quantity"), Bytes.toBytes(quantity))
      .addColumn(cf, toBytes("invoiceDate"), Bytes.toBytes(invoiceDate.getTime))
      .addColumn(cf, toBytes("unitPrice"), Bytes.toBytes(unitPrice))
      .addColumn(cf, toBytes("customerId"), Bytes.toBytes(customerId))
      .addColumn(cf, toBytes("country"), Bytes.toBytes(country))
  }
}


object SalesRecord{

  var parser: CsvParser = _
  var dateFormatter: SimpleDateFormat = _

  def init(): Unit ={
    val settings = new CsvParserSettings()
    settings.getFormat.setLineSeparator("\n")
    parser = new CsvParser(settings)
  }

  def toDate(s:String): java.sql.Date = {
    if(dateFormatter == null){
      dateFormatter = new SimpleDateFormat("MM/dd/yyyy")
    }
    val date = dateFormatter.parse(s)
    return new Date(date.getTime)
  }


  def parseMultiLines(lines: Seq[String]): Seq[SalesRecord] = {
    lines.map(parse)
  }

  def parse(line: String): SalesRecord = {
    if(parser == null){
      init()
    }
    val tokens = parser.parseLine(line)
    SalesRecord(
      tokens(0).toLong
      , tokens(1)
      , tokens(2)
      , tokens(3).toInt
      , toDate(tokens(4))
      , tokens(5).toDouble
      , tokens(6).toLong
      , tokens(7)
    )
  }

}