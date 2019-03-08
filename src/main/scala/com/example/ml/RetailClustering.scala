package com.example.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.{KMeans, KMeansModel, LDA, LDAModel}
import org.apache.spark.ml.feature.{CountVectorizer, StandardScaler, Tokenizer, VectorAssembler}

object RetailClustering {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local")

    val spark:SparkSession = SparkSession.builder()
      .config(conf).getOrCreate()


    val pathSalesData = args(0)

    val options = Map("header" -> "true"
      , "inferSchema" -> "true")

    val sales = spark.read.options(options).csv(pathSalesData).cache()

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("Quantity", "UnitPrice"))
      .setOutputCol("features")

    val standardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("features_std")

    val km = new KMeans()
      .setK(5)
      .setFeaturesCol("features_std")

    val pipe = new Pipeline()
      .setStages(Array(vectorAssembler, standardScaler, km))

    val model = pipe.fit(sales)

    var modelPath = "/tmp/spark-retail-model"
    if(args.size > 1){
      modelPath = args(1)
    }

    model.write.overwrite().save(modelPath)

    val kmModel = model.stages(2).asInstanceOf[KMeansModel]
    val summary = kmModel.summary
    print("Cluster size: ")
    summary.clusterSizes.foreach(println)


    println("Cluster Centers: ")
    kmModel.clusterCenters.foreach(println)

    val savedModel = PipelineModel.load(modelPath)

    val prediction = savedModel.transform(sales)
    prediction.show()

    prediction.groupBy("prediction").count().show()


    /*
    val tokenizer = new Tokenizer().setInputCol("Description").setOutputCol("Description_tokenized")

    val countVectorizer = new CountVectorizer()
      .setInputCol("Description_tokenized")
      .setOutputCol("features")
      .setVocabSize(500)
      .setMinTF(0)
      .setMinDF(0)
      .setBinary(true)

    val lda = new LDA().setK(10).setMaxIter(5).setFeaturesCol("features")

    val descriptionClusteringPipe = new Pipeline().setStages(Array(tokenizer, countVectorizer, lda))

    val descriptionClusteringModel = descriptionClusteringPipe.fit(sales)

    val ldaModel = descriptionClusteringModel.stages(2).asInstanceOf[LDAModel]

    ldaModel.describeTopics(3).show()
  */
  }
}
