package edu.bupt.iot.spark.streaming

import edu.bupt.iot.util.hdfs.HdfsConfig
import edu.bupt.iot.util.json.JSONParse
import edu.bupt.iot.util.kafka.KafkaConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DataStream {
  def kafkaValue2Str(str: String): String = {
    val line = new StringBuilder
    try {
      val json = JSONParse.str2Json(str)
      val map = JSONParse.json2Map(json)
      val list = JSONParse.json2List(map.get("data"))
      list.foreach(dataMap => {
        //一条kafka消息中可能有多组数据
        line.append(map("tenantId").asInstanceOf[Number].longValue.toString + ","
          + dataMap("key").toString + ","
          + map("deviceId").toString + ","
          + dataMap("value").asInstanceOf[Number].doubleValue().toString + ","
          + dataMap("ts").asInstanceOf[Number].longValue.toString + ";")
      })
      //val dataMap = list.head
      line.toString
    } catch {
      case e : Throwable =>
        println(e)
        e.printStackTrace()
        line.toString
    }
  }
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", HdfsConfig.getUserName())
    val conf = new SparkConf().setAppName("DataStream").setMaster("spark://master:7077")//("spark://10.108.218.64:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "edu.bupt.iot.util.kafka.ConsumerRecordRegistrator")

    /*
    val checkpointDir = "checkpoint/"//"hdfs://master:9000/data/checkpoint/"
    def createStreamingContext(): StreamingContext = {
      val ssc = new StreamingContext(conf, Seconds(5))
      ssc.checkpoint(checkpointDir)
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)

    */
    val ssc = new StreamingContext(conf, Seconds(300))
    val topics = Array("deviceData")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, KafkaConfig.getConsumerConf())
    )
    val kafkaValueStream = kafkaStream.map(record => {record.value()})
      .map(record => kafkaValue2Str(record))
      .filter(!_.equals(""))
      .flatMap(_.split(";"))
    kafkaValueStream.foreachRDD(rdd =>
      rdd.foreach(item => {
        println(item)
      })
    )
    val win = kafkaValueStream.window(Seconds(3600), Seconds(3600))
      .transform(rdd => rdd.distinct())
    //win.print()
    win.saveAsTextFiles("hdfs://master:9000/data/device-data")

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
