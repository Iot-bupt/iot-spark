package edu.bupt.iot.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val inputFile = "hdfs://master:9000/test/word.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val test = textFile.flatMap(_.split("\t")).map(item => (item(0), item(1).toInt))

    val wordCount = textFile.flatMap(_.split(" ")).map((_, 1))
      .reduceByKey((_+_))
      .sortBy((_._2),ascending = false)
    wordCount.foreach(println(_))
  }
}
