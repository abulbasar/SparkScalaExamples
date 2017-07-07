import org.apache.spark.{SparkContext, SparkConf}


object WordCount {
  def main(args: Array[String]): Unit = {
    val path = "/etc/passwd"
    val conf = new SparkConf()

    conf.setAppName("sample demo")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.textFile(path).
      flatMap(_.split("\\W+")).
      filter(_.length > 0).
      map((_, 1)).
      reduceByKey(_+_).
      collect.
      foreach(println)
  }
}