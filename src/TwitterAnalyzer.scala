

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.{ KafkaUtils, OffsetRange, HasOffsetRanges }
import org.apache.spark.storage.StorageLevel
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions._


object TwitterAnalyzer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("DirectKafkaWordCount").
      setMaster("local[*]")
      
    val sc = new SparkContext(conf)
      
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val ssc = new StreamingContext(sc, Seconds(5))
    val kafkaParams = PropertiesLoader.loadAsMap("kafka.properties")
    val appParams = PropertiesLoader.loadAsMap("app.properties")
    val topic = appParams.get("topic").get
    val topics = Array(topic).toSet
    
    
    
    val toTags = udf{(msg: String) => 
        val tokens = msg.toLowerCase.split("\\s+").filter(_.startsWith("#"))
        tokens
    }



    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //messages.map(_._2).print()
    
    messages.map(_._2).foreachRDD{rdd =>
      
      if(!rdd.isEmpty()){
        sqlContext
        .read
        .json(rdd)
        .select("text")
        .select(toTags(col("text")).alias("tags"))
        .select(explode(col("tags")).alias("tag"))
        .groupBy("tag")
        .count
        .orderBy(desc("count"))
        .show(5)
      
      }
      
    }
    
    //.saveAsTextFiles(topic, "txt")

    /*var offsetRanges = Array[OffsetRange]()
    messages.transform{ rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      rdd
    }.map(_._2).print()*/

    ssc.start()
    ssc.awaitTermination()
  }
}