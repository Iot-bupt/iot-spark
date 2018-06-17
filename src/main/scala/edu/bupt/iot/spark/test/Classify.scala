package edu.bupt.iot.spark.test

import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object Classify {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Classify")
      .getOrCreate()
    import spark.implicits._
    val rawData = spark.sparkContext.textFile("hdfs://master:9000/test/classify/train_noheader.tsv")
      .map(_.split("\t"))

    val data = rawData.map{ r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1)
        .map(d => if (d == "?") 0.0 else d.toDouble)
      (label, Vectors.dense(features))
    }.toDF("label", "features")
    val nbData= rawData.map{ r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1)
        .map(d => if (d == "?") 0.0 else d.toDouble)
        .map(d => if (d < 0) 0.0 else d)
      (label, Vectors.dense(features))
    }.toDF("label", "features")
    data.cache()
    println(data.count)
    val lrModel = new LogisticRegression().setMaxIter(10).fit(data)
    val nbModel = new NaiveBayes().fit(nbData)
    val dtModel = new DecisionTreeClassifier().setMaxDepth(5).fit(data)
    data.createOrReplaceTempView("data")
    nbData.createOrReplaceTempView("nbData")
    lrModel.transform(spark.sql("select features from data")).show(100)
    nbModel.transform(spark.sql("select features from nbData")).show(100)
    dtModel.transform(spark.sql("select features from data")).show(100)
  }
}
