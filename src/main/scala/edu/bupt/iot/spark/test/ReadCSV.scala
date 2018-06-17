package edu.bupt.iot.spark.test

import edu.bupt.iot.util.kafka.{KafkaConfig, KafkaSink}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object ReadCSV {
  def main(args: Array[String]): Unit = {
    val inputFile = "hdfs://39.104.186.210:9000/data/-1527240410000"
    val conf = new SparkConf().setAppName("ReadCSV").setMaster("local")//("spark://10.108.218.64:7077")
    val sc = new SparkContext(conf)
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = KafkaConfig.getProducerConf()
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    val input = sc.textFile(inputFile)
    //input.saveAsTextFile("test2")

    input.foreach( line => {
      println(line)
      /*
      val tmp = line.split(",")
      kafkaProducer.value.send("test", "movie id: " + tmp(0)
        + ", user id: " + tmp(1)
        + ", score: " + tmp(2)
        + ", time: " + tmp(3))
        */
//      println("movie id: " + tmp(0)
//        + ", user id: " + tmp(1)
//        + ", score: " + tmp(2)
//        + ", time: " + tmp(3))
    })

  }
}
