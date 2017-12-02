package com.einext.airlines

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1

/*
* Usage: find cause of airline delay based on 5 fields. Find the field name which has non-zero highest value.
* Use this UDF for faster execution on larger dataset.
*
* sqlContext.registerJavaFunction("findDelayCause", "com.einext.airlines.findDelayCause")
* data.selectExpr("findDelayCause(struct(*))").show()
*
* */

class findDelayCause extends UDF1[Row, String] {

  @throws[Exception]
  override def call(row: Row): String = {
    val fields = "CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay".split(",").map(_.trim)
    val values = fields.map{field =>
      val v: String = row.getString(row.fieldIndex(field))
      if (v != null && v.trim.length > 0) v.trim.toDouble else -1.0
    }
    var cause:String = null
    if(values.max > 0){
      val i = values.indexOf(values.max)
      cause = fields(i)
    }
    cause
  }
}
