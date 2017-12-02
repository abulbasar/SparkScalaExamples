import org.apache.spark.sql.SparkSession
import import java.util.concurrent.Executors
import scala.concurrent._
import scala.concurrent.duration._

object ConcurrentApp {
  def def appMain(args: Array[String]) = {

    val spark = SparkSession
        .builder
        .appName(getClass.getName)
        .getOrCreate()
        
    val pool = Executors.newFixedThreadPool(5)
    
    implicit val xc = ExecutionContext.fromExecutorService(pool)
    
    val df = spark.sparkContext.parallelize(1 to 100).toDF
    val tasks = Seq(
      taskA(df, "hdfs:///dis.parquet"),
      taskB(df, "hdfs:///dis.parquet")
    )
    Await.result(Future.sequence(tasks), Duration(1, MINUTES))
  }

  def taskA(df: DataFrame, outPath: String)(implicit xc: ExecutionContext) = Future {
    df.distinct.write.parquet(outPath)
  }

  def taskB(df: DataFrame, outPath: String)(implicit xc: ExecutionContext) = Future {
    df.agg(sum("value")).write.parquet(outPath) 
  }
}
