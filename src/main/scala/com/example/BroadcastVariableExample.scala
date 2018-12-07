package com.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

object BroadcastVariableExample {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local")


    val spark = SparkSession
      .builder()
      .config(conf).getOrCreate()

    val sql = spark.sql _


    /*

    * Auto broadcast is enabled by default
    * If the size of table less than spark.sql.autoBroadcastJoinThreshold, the table is broadcast to all executors
    * Broadcast tables are cached automatically
    * Default size of spark.sql.autoBroadcastJoinThreshold is 10 MB
    * spark.sql.autoBroadcastJoinThreshold = -1 disables the auto broadcast
    * In practice, spark comes to know about the size of table from the table statistics if available
    * In real cluster keeping spark.sql.autoBroadcastJoinThreshold to default value is a good practice
    *
    * */

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


    val csvOptions = Map("header" -> "true"
      , "inferSchema" -> "true" )

    val sp500 = spark.read.options(csvOptions).csv("/data/SP500.csv")

    sp500.show()
    sp500.printSchema()


    val stocks = spark.read.options(csvOptions).csv("/data/stocks.csv")
    stocks.printSchema()
    stocks.show()


    /* Join two dataframes by without broadcasting */
    val withoutBroadCast = stocks.join(sp500, usingColumn = "symbol")

    /* Join two dataframes by broadcasting the smaller dataframe */
    val withBroadCast = stocks.join(F.broadcast(sp500), usingColumn = "symbol")



    println("Explain plan without broadcasting")
    withoutBroadCast.explain()

    println("Explain plan with broadcasting")
    withBroadCast.explain()




    /* Open the Spark web UI and see the number stages and shuffle
     data volume between the following two operations
     */
    withoutBroadCast.collect()
    withBroadCast.collect()




    stocks.createOrReplaceTempView("stocks")
    sp500.createOrReplaceTempView("sp500")


    sql("show tables").show()

    val withoutBroadcastSQL =  sql("select * from stocks t1 join sp500 t2 on t1.symbol = t2.symbol")
    println("Explain plan without broadcasting (SQL)")
    withoutBroadcastSQL.explain()


    val withBroadcastSQL =  sql("select /*+ MAPJOIN(t2) */ * from stocks t1 join sp500 t2 on t1.symbol = t2.symbol")
    println("Explain plan with broadcasting (SQL)")
    withBroadcastSQL.explain()


    withoutBroadcastSQL.collect()
    withBroadcastSQL.collect()

    println("Process is complete. Please enter to exit.")
    System.in.read()
    spark.close()

  }

}
