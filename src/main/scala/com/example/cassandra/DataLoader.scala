package com.example.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object DataLoader {

  private var spark: SparkSession = _
  private var sql:(String) => Dataset[Row] = _

  def loadAs(file: String, table: String, cache: Boolean = true) {
    val options = Map(
      "header" -> "true",
      "inferSchema" -> "true")

    spark
      .read
      .format("csv")
      .options(options)
      .load(file)
      .createOrReplaceTempView(table)

    if (cache) {
      spark.table(table).cache()
    }
  }

  def query(stmnt: String, table:String){
    spark.sql(stmnt).createOrReplaceTempView(table)
  }

  def lowerCaseColumns(ds:Dataset[Row]):Dataset[Row] =  {

    var df = ds
    ds.columns.foreach { col =>
      df = df.withColumnRenamed(col, col.toLowerCase())
    }
    df
  }

  def saveToCassandra(sparkTable:String, cassandraTable:String){
     lowerCaseColumns(spark.table(sparkTable))
     .write
     .format("org.apache.spark.sql.cassandra")
     .mode(SaveMode.Append)
     .options(Map( "table" -> cassandraTable, "keyspace" -> "demo"))
     .save()
  }

  def loadCassandraTable(cassandraTable:String, sparkTable:String){
     spark
     .read
     .format("org.apache.spark.sql.cassandra")
     .options(Map( "table" -> cassandraTable, "keyspace" -> "demo"))
     .load()
     .createOrReplaceTempView(sparkTable)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
      .setIfMissing("spark.cassandra.auth.username", "cassandra")
      .setIfMissing("spark.cassandra.auth.password", "pass123")


    spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    sql = spark.sql _

    /*
     loadAs("/home/training/Downloads/datasets/ml-latest-small/movies.csv", "movies")
     loadAs("/home/training/Downloads/datasets/ml-latest-small/ratings.csv", "ratings")

     sql("show tables").show()

     val users = spark
     .read
     .format("org.apache.spark.sql.cassandra")
     .options(Map( "table" -> "user", "keyspace" -> "demo"))
     .load()

     users.show()

     saveToCassandra("movies", "movies")
     saveToCassandra("ratings", "ratings")

     */

     loadCassandraTable("movies", "movies")
     loadCassandraTable("ratings", "ratings")

     val df = sql("""
       select t1.movieid, t1.title, avg(t2.rating) avg_rating
       from movies t1 join ratings t2 on t1.movieid = t2.movieid
       group by t1.movieid, t1.title having count(t2.movieid) >= 100
       order by avg_rating desc
       """)
     df.show()

  }

}
