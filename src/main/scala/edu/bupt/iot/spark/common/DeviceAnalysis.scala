package edu.bupt.iot.spark.common

import edu.bupt.iot.util.kafka.KafkaConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks
import scala.util.parsing.json.JSONObject

object DeviceAnalysis {
  def main(args: Array[String]): Unit = {
    //init tenantId, deviceType, deviceId, startTime ,endTime
    var tenantId = "-1"
    var startTime = -1.toLong
    var endTime = -1.toLong //System.currentTimeMillis()
    //set tenantId, startTime, endTime according to args
    if (args.length > 0) tenantId = args(0)
    if (args.length > 1) startTime = args(1).toLong
    if (args.length > 2) endTime = args(2).toLong
    if (startTime < 0.toLong) startTime = 0.toLong
    if (endTime < 0.toLong) endTime = System.currentTimeMillis()
    args.foreach(println(_))
    val dataFilePre = { //find data file pre according to start and end time
      //timestamps to String
      //start time advance 1 hour
      val start = new StringFormat((if (startTime > 3600000) startTime - 3600000 else 0).toString).formatted("%13s").replaceAll(" ", "0")
      //String.format("%13d", startTime - 3600000)
      //end time delay 1 hour
      val end = new StringFormat((endTime + 3600000).toString).formatted("%13s").replaceAll(" ", "0")
      val pre = {
        var endPos = 0
        val loop = new Breaks
        loop.breakable{
          //loop to find pre endPos
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
    //read all file from hdfs
    val inputFiles = s"hdfs://master:9000/data/device-data-${dataFilePre}*"
    //val inputFiles = s"hdfs://master:9000/test/rec*"
    val conf = new SparkConf().setAppName("UserDeviceAnalysis").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    //filter data with tenantId, deviceType, deviceId
    var rawData = sc.textFile(inputFiles).map(_.split(","))
    //println(rawData.count())
    if (!tenantId.equals("-1")) rawData = rawData.filter(item => item(0).equals(tenantId))
    //println(rawData.count())
    //keep value with time
    val data = rawData.map(item => (item(1), item(2), item(3), item(4).toLong))
      .filter(item => item._4 >= startTime && item._4 <= endTime)
      .map(item => ((item._1, item._2), item._3.toDouble))
      .cache()
    //keep value to cal statistics info
    val info = {
      var tmp = ""
      if (!data.isEmpty()) {
        //data.foreach(println(_))
        //val count = data.count
        val dataCount = data.map(item => (item._1._1, 1)).countByKey()
        //val dataCountBd = sc.broadcast(dataCount)

        val deviceCount = data.mapValues(x => 0)
          .reduceByKey((x, y) => x + y)
          .map(item => (item._1._1, item._1._2))
          .countByKey()
        //val deviceCountBd = sc.broadcast(deviceCount)

        val mean = data.mapValues(x => (1, x))
          .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
          .mapValues(value =>  value._2.toDouble / value._1.toDouble)
          //.map{case (key, value) => (key,  value._2.toDouble / value._1.toDouble)}
          .collectAsMap()
        val meanBd = sc.broadcast(mean)  //meanBd.value

        val stdev = data.map{case (key, value) => (key, value - meanBd.value(key))}
          .mapValues(value => (1, value * value))
          .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
          .mapValues(value =>  math.sqrt(value._2.toDouble / value._1.toDouble))
          //.map{case (key, value) => (key,  math.sqrt(value._2.toDouble / value._1.toDouble))}
          .collectAsMap()
        val stdevBd = sc.broadcast(stdev)

        //stdev.foreach(println(_))
        val usualDataCount = data.filter{case (key ,value) =>
          (value - meanBd.value(key)).abs <= 3 * stdevBd.value(key)}
          .map(item => (item._1._1, 1))
          .countByKey()

        //val usualDataCountBd = sc.broadcast(usualDataCount)

        val usualDataRate = {
          val tmp = mutable.Map[String, Any]()
          usualDataCount.foreach(
            item =>
              tmp.put(item._1, usualDataCount(item._1) / dataCount(item._1).toDouble)
          )
          tmp
        }
        tmp = JSONObject(Map(
          "status" -> "success",
          "data" -> JSONObject(Map(
            "deviceCount" -> JSONObject(deviceCount.toMap),
            "dataCount" -> JSONObject(dataCount.toMap),
            "usualDataCount" -> JSONObject(usualDataCount.toMap),
            "usualDataRate" -> JSONObject(usualDataRate.toMap)
          ))
        )).toString()
      } else{
        tmp = "{\"status\":\"No Matching Data\"}"
      }
      tmp
    }

    println(info)
    //kafka send message
    val producer = new KafkaProducer[String, String](KafkaConfig.getProducerConf())
    producer.send(new ProducerRecord[String, String](tenantId+"_device", "Statistics", info))
    producer.close()
  }
}
