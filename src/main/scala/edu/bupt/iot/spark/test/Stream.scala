package edu.bupt.iot.spark.test

import edu.bupt.iot.util.hdfs.{HdfsConfig, HdfsUtils}
import edu.bupt.iot.util.kafka.KafkaConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Stream {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", HdfsConfig.getUserName())
    val conf = new SparkConf().setAppName("Stream").setMaster("local")//("spark://10.108.218.64:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "edu.bupt.iot.util.kafka.ConsumerRecordRegistrator")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topics = Array("test", "test_1")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, KafkaConfig.getConsumerConf())
    )
    val kafkaValueStream = kafkaStream.map(record => {
      record.value()
    })
    val win = kafkaStream.window(Seconds(20), Seconds(20))
    win.saveAsTextFiles("hdfs://master:9000/test/tmp")
    //kafkaStream.saveAsTextFiles("hdfs://master:9000/test/tmp","txt")
    /*
    kafkaStream.foreachRDD( rdd =>
      rdd.foreach( record =>
        HdfsUtils.append("/test/test_kafka.txt", record.toString)
      )
    )
    */
    //kafkaStream.map(record => (record.key, record.value))
//    val lines = ssc.socketTextStream("localhost", 7777)
//    //val errorLines = lines.filter(line => line.contains("error"))
//    val errorLines = lines.filter(_.contains("error"))
//    errorLines.print()
//    //errorLines.saveAsTextFiles("StreamTest", "")

    ssc.start()
    ssc.awaitTermination()
  }
}
