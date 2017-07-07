import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds 
import org.apache.spark.storage.StorageLevel


object StreamApp {
  def main(args:Array[String]) {
    val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local[*]")
    
    val sc = new SparkContext(conf)
    
    val ssc = new StreamingContext(sc, Seconds(5))
    
    val raw = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
    
    raw.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}