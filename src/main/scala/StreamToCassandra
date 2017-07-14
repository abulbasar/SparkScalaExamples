import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext


object StreamToCassandra {
  def main(args:Array[String]) {
    val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local[*]")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    
    val sc = new SparkContext(conf)
    val sqlContex = new SQLContext(sc)
    import sqlContex.implicits._
    
    val ssc = new StreamingContext(sc, Seconds(5))
    
    val raw = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
    
    raw.foreachRDD{rdd =>
      if(!rdd.isEmpty()){
        val df = sqlContex.read.json(rdd)
        .filter(not(isnull(col("id"))))
        .drop("_corrupt_record")
        
        df.show()
        
        df
        .write
        .format("org.apache.spark.sql.cassandra")
        .option("table", "tweets")
        .option("keyspace", "demo")
        .mode("append")
        .save()
      }    
    }

    //raw.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}
