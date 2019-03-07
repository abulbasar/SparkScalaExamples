package com.example.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


abstract class GenericUDAF extends UserDefinedAggregateFunction {

  def inputSchema: StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  def bufferSchema: StructType = StructType(
    StructField("window_list", ArrayType(DoubleType, false)) :: Nil
  )

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new ArrayBuffer[Double]()
  }

  def update(buffer: MutableAggregationBuffer,input: org.apache.spark.sql.Row): Unit = {
    var bufferVal = buffer.getAs[mutable.WrappedArray[Double]](0).toBuffer
    bufferVal+=input.getAs[Double](0)
    buffer(0) = bufferVal
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: org.apache.spark.sql.Row): Unit = {
    buffer1(0) = buffer1.getAs[ArrayBuffer[Double]](0) ++ buffer2.getAs[ArrayBuffer[Double]](0)
  }

  def dataType: DataType
  def evaluate(buffer: Row): Any

}