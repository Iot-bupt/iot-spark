package edu.bupt.iot.util.kafka

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.kafka010.KafkaUtils

object Test {

  def main(args: Array[String]): Unit = {

    val inputFile = "hdfs://master:9000/test/movie_imdb.csv"
    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = KafkaConfig.getProducerConf()
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_+_)
      .sortBy(_._2,ascending = false)
    wordCount.foreach( record => {
        kafkaProducer.value.send("deviceData", record._1.toString + ":" + record._2.toString)
    })
  }

}
