package edu.bupt.iot.spark.test

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
object MovieRe {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MovieRe")
      .master("spark://10.108.218.64:7077")
      .getOrCreate()
    import spark.implicits._
    val rawData = spark.sparkContext.textFile("hdfs://master:9000/data/ml-100k/u.data")
      .map(_.split("\t").take(3))
      .map{case Array(user, movie, rating) =>
      (user.toInt, movie.toInt, rating.toFloat)}.toDF("user", "item", "rating")
    //val df = spark.createDataFrame(rawData)

    val Array(training, test) = rawData.randomSplit(Array(0.8, 0.2))

    val als = new ALS().setMaxIter(10).setRank(50).setRegParam(0.01)
    val pipeline = new Pipeline().setStages(Array(als))
    val modelP = pipeline.fit(training)
//    val model = als.fit(training)
//    model.save("hdfs://master:9000/test/model")
//    val saveModel = ALSModel.load("hdfs://master:9000/test/model")
//    var ret = saveModel.transform(test)
    //ret.show(10)
    val ret = modelP.transform(test)

    ret.createOrReplaceTempView("ret")
    spark.sql("select * from ret where user = 1 and item < 10").show()
    ret.select("user", "item" , "rating", "prediction").show(100)
//    ret = saveModel.recommendForAllUsers(5)
//    ret = saveModel.recommendForUserSubset(test, 5)

    //ret.foreach(println(_))
  }
}
