package com.example.streaming.kafka

import java.io.{BufferedOutputStream, FileOutputStream}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object StructuredKafkaStreamAvro {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._


    val source = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "mysql-user")
      .load()

    source.printSchema()


    lazy val schema = new Schema.Parser().parse(
      """
        |{
        |    "type": "record",
        |    "name": "user",
        |    "fields": [
        |        { "name": "id", "type": "int" },
        |        { "name": "name", "type": "string" }
        |    ]
        |}
        |
      """.stripMargin)

    lazy val reader = new GenericDatumReader[GenericRecord](schema)


    val enriched = source
      .select("value")
      .as[Array[Byte]]
      .map { bytes =>

        val bos = new BufferedOutputStream(new FileOutputStream("/home/cloudera/sample.avro"))
        bos.write(bytes)
        bos.close()

        val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
        val record = reader.read(null, decoder)
        (record.get("id").toString, record.get("name").toString)
      }

    enriched
      .writeStream
      .format("console")
      .start()

    spark.streams.awaitAnyTermination()
    spark.close()

  }

}