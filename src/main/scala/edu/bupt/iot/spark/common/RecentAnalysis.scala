package edu.bupt.iot.spark.common

import org.apache.spark.sql.SparkSession
import java.util.Date

import edu.bupt.iot.util.kafka.KafkaConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.control.Breaks

object RecentAnalysis {
  def main(args: Array[String]): Unit = {
    var endTime = new Date().getTime
    if(args.length > 0) endTime = args(0).toLong
    val startTime = endTime - 7 * 3600 * 24 * 1000
    println(endTime, startTime)
    val dataFilePre = {
      val start = new StringFormat((if (startTime > 3600000) startTime - 3600000 else 0).toString).formatted("%13s").replaceAll(" ", "0")
      val end = new StringFormat((endTime + 3600000).toString).formatted("%13s").replaceAll(" ", "0")
      val pre = {
        var endPos = 0
        val loop = new Breaks
        loop.breakable{
          for(i <- 0 to 13){
            if(start.charAt(i) != end.charAt(i)){
              endPos = i
              loop.break
            }
          }
        }
        start.substring(0, endPos)
      }
      pre
    }
    println(dataFilePre)
    //val inputFiles = s"hdfs://master:9000/data/device-data-${dataFilePre}*"
    val inputFiles = s"hdfs://master:9000/data/1527782400000"
    println(inputFiles)
    val spark = SparkSession
      .builder()
      .appName("RecentAnalysis")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile(inputFiles)
      .map(_.split(","))
      .map{case Array(tenant_id, key, device_id, value, time_stamp) =>
        (tenant_id.toInt, key, device_id, value.toDouble, time_stamp.toLong)}
      //.filter(item => item._4 >= startTime && item._4 < endTime)
      .toDF("tenant_id", "key", "device_id", "value", "time_stamp")
    data.createOrReplaceTempView("data")
    //println(data.show(10))
    val producer = new KafkaProducer[String, String](KafkaConfig.getProducerConf())
    spark.sql("select tenant_id, device_id, key as value_type," +
      " max(value) as max_value, min(value) as min_value," +
      " mean(value) as mean_value, stddev(value) as stddev_value, " +
      " count(*) as data_count" +
      " from data" +
      " group by tenant_id, key, device_id")
      .rdd.cache()
      .map(item =>
        (item(0).toString.toInt, item(1).toString, item(2).toString,
          item(3).toString.toDouble, item(4).toString.toDouble,
          item(6).toString.toDouble, item(6).toString.toDouble,
          item(7).toString.toInt).toString())
      .collect()
      .foreach(item => {
        producer.send(new ProducerRecord[String, String]("recent", "recent", item))
      })
    producer.close()
  }
}
