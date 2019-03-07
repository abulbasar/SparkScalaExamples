package com.example.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable


class DecilesUDAF extends GenericUDAF {

  override def dataType: DataType = ArrayType(DoubleType, false)

  override def evaluate(buffer: Row): Any = {
    val sortedWindow = buffer.getAs[mutable.WrappedArray[Double]](0).sorted.toBuffer
    val windowSize = sortedWindow.size
    if (windowSize == 0) return null
    if (windowSize == 1) return (0 to 10).map(_ => sortedWindow.head).toArray

    (0 to 10).map(i => sortedWindow(Math.min(windowSize-1, i*windowSize/10))).toArray

  }
}