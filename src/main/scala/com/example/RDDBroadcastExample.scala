package com.example

import org.apache.spark.{SparkConf, SparkContext}

object RDDBroadcastExample {


  def main(args: Array[String]): Unit = {

    val moviesPath = args(0)
    val ratingsPath = args(1)


    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local")

    val sc = new SparkContext(conf)


    val movies = sc.textFile(moviesPath)
    val ratings = sc.textFile(ratingsPath)

    val movieIdTitle = movies
      .filter(!_.startsWith("movieId"))
      .map(_.split(","))
      .map(tokens => (tokens(0), tokens(1)))

    val movieIdAvgRating = ratings
      .filter(!_.startsWith("userId"))
      .map(_.split(","))
      .map(tokens => (tokens(1), tokens(2).toDouble))
      .groupByKey()
      .mapValues(values => values.sum/values.size)


    println(movieIdTitle.collectAsMap())


    val broadCastMovies = sc.broadcast(movieIdTitle.collectAsMap())


    println(">>>> Output using RDD join operation")
    movieIdAvgRating
      .join(movieIdTitle)
      .take(10)
      .foreach(println)

    println(">>>> Output using broadcast variable")
    movieIdAvgRating
      .map(p => {
        val movieId = p._1
        val avgRating = p._2
        val title = broadCastMovies.value.get(movieId).getOrElse(null)
        (movieId, (avgRating, title))
      })
      .take(10)
      .foreach(println)

  }

}
