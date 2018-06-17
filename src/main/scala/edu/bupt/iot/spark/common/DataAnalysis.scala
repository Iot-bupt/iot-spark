package edu.bupt.iot.spark.common

import edu.bupt.iot.util.kafka.KafkaConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks
import scala.util.parsing.json.{JSONArray, JSONObject}

object DataAnalysis {
  def main(args: Array[String]): Unit = {
    //init tenantId, deviceType, deviceId, startTime ,endTime
    var tenantId, deviceType, deviceId = "-1"
    var startTime = -1.toLong
    var endTime = -1.toLong //System.currentTimeMillis()
    var partNum = 1
    //set tenantId, deviceType, deviceId, startTime, endTime according to args
    if (args.length > 0) tenantId = args(0)
    if (args.length > 1) deviceType = args(1)
    if (args.length > 2) deviceId = args(2)
    if (args.length > 3) startTime = args(3).toLong
    if (args.length > 4) endTime = args(4).toLong
    if (args.length > 5) partNum = args(5).toInt
    if (startTime < 0.toLong) startTime = 0.toLong
    if (endTime < 0.toLong) endTime = System.currentTimeMillis()
    if (partNum <= 0) partNum = 1

    val gap = math.ceil((endTime-startTime).toDouble / partNum.toDouble).toLong

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
    //println(s"hdfs://master:9000/data/${dataFilePre}*")
    val inputFiles = s"hdfs://master:9000/data/device-data-${dataFilePre}*"
    println(inputFiles)
    //val inputFiles = s"hdfs://master:9000/test/rec*"
    val conf = new SparkConf().setAppName("UserDataAnalysis").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    /*
    val data1 = sc.textFile(inputFile)
      .map(line => (line.split(",")(0), line.split(",")(1), line.split(",")(2)))
      .filter(item => item._1.equals("500"))
      .map(_._3.toDouble).cache()
     */
    //filter data with tenantId, deviceType, deviceId
    var rawData = sc.textFile(inputFiles).map(_.split(","))

    if (!deviceId.equals("-1")) rawData = rawData.filter(item => item(2).equals(deviceId))
    if (!tenantId.equals("-1")) rawData = rawData.filter(item => item(0).equals(tenantId))
    if (!deviceType.equals("-1")) rawData = rawData.filter(item => item(1).equals(deviceType))
    val data = rawData.map(item => (item(3).toDouble, item(4).toLong))
      .filter(item => item._2 >= startTime && item._2 < endTime)
      .map(item =>  ((startTime + (item._2 - startTime) / gap * gap).toString, item._1))
      .cache()
   // data.foreach(println(_))
    val info = {
      var tmp = ""
      if (!data.isEmpty()) {
        //data.foreach(println(_))
        val max = data.reduceByKey((x, y) => math.max(x,y)).collectAsMap()
        val min = data.reduceByKey((x, y) => math.min(x,y)).collectAsMap()

        val count = data.countByKey()
        val mean = data.mapValues(x => (1, x))
          .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
          .mapValues(value =>  value._2.toDouble / value._1.toDouble)
          .collectAsMap()
        val meanBd = sc.broadcast(mean)  //meanBd.value

        val stdev = data.map{case (key, value) => (key, value - meanBd.value(key))}
          .mapValues(value => (1, value * value))
          .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
          .mapValues(value =>  math.sqrt(value._2.toDouble / value._1.toDouble))
          .collectAsMap()
        val stdevBd = sc.broadcast(stdev)

        val usualCount = data.filter{case (key ,value) =>
          (value - meanBd.value(key)).abs <= 3 * stdevBd.value(key)}
          .map(item => (item._1, 1))
          .countByKey()

        val usualRate = {
          val tmp = mutable.Map[String, Any]()
          usualCount.foreach(
            item =>
              tmp.put(item._1, usualCount(item._1) / count(item._1).toDouble)
          )
          tmp
        }

        tmp = {
          val all  = mutable.Map[String, Any]()
          all.put("status", "success")

          val list = List[Any](
            JSONObject(max.toMap),
            JSONObject(min.toMap),
            JSONObject(mean.toMap),
            JSONObject(stdev.toMap),
            JSONObject(count.toMap),
            JSONObject(usualCount.toMap),
            JSONObject(usualRate.toMap)
          )
          all.put("data", JSONArray(list))

          /*
          val map =  Map(
            "max" -> JSONObject(max.toMap),
            "min" -> JSONObject(min.toMap),
            "mean" -> JSONObject(mean.toMap),
            "stdev" -> JSONObject(stdev.toMap),
            "count" -> JSONObject(count.toMap),
            "usualCount" -> JSONObject(usualCount.toMap),
            "usualRate" -> JSONObject(usualRate.toMap)
          )
          all.put("data", JSONObject(map))
          */
          /*
          val list = List[Any](
            JSONObject(Map("max" -> JSONObject(max.toMap))),
            JSONObject(Map("min" -> JSONObject(min.toMap))),
            JSONObject(Map("mean" -> JSONObject(mean.toMap))),
            JSONObject(Map("stdev" -> JSONObject(stdev.toMap))),
            JSONObject(Map("count" -> JSONObject(count.toMap))),
            JSONObject(Map("usualCount" -> JSONObject(usualCount.toMap))),
            JSONObject(Map("usualRate" -> JSONObject(usualRate.toMap)))
          )
          all.put("data", JSONArray(list))
          */
          /*
          val part  = mutable.Map[String, Any]()
          count.keys.foreach(key => {
            val partData = JSONObject(Map[String, Any](
              "count" -> count(key),
              "max" -> max(key),
              "min" -> min(key),
              "mean" -> mean(key),
              "stdev" -> stdev(key),
              "usualCount" -> usualCount(key),
              "usualRate" -> usualRate(key)
            ))
            part.put(key, partData)
            }
          )
          all.put("data", part)
          */
          JSONObject(all.toMap).toString()
        }
      } else{
        tmp = "{\"status\":\"No Matching Data\"}"
      }
      tmp
    }
    println(info)
    //kafka send message
    val producer = new KafkaProducer[String, String](KafkaConfig.getProducerConf())
    producer.send(new ProducerRecord[String, String](tenantId+"_data", "Statistics", info))
    producer.close()

    /*
    //keep value with time
    val dataWithTime = rawData.map(item => (item(3).toDouble, item(4).toLong))
      .filter(item => item._2 >= startTime && item._2 <= endTime)
      .cache()
    //keep value to cal statistics info
    val data = dataWithTime.map(_._1).cache()
    val info = {
      var tmp = ""
      if (!data.isEmpty()) {
        //data.foreach(println(_))
        val count = data.count
        val sum = data.sum
        val mean = data.mean
        val stdev = data.stdev
        val max = data.collect.max
        val min = data.collect.min
        //statistics info to json str
        val usualRate = data.filter(value =>
          (value - mean).abs <= 3 * stdev).count().toDouble /
          data.count().toDouble
        tmp = JSONObject(Map[String, Any](
          "status" -> "success",
          "count" -> count,
          "mean" -> mean,
          "sum" -> sum,
          "stdev" -> stdev,
          "max" -> max,
          "min" -> min,
          "normalRate" -> usualRate
        )).toString()
      } else{
        tmp = "{\"status\":\"No Matching Data\"}"
      }
      tmp
    }
    println(info)
    //kafka send message
    val producer = new KafkaProducer[String, String](KafkaConfig.getProducerConf())
    producer.send(new ProducerRecord[String, String](tenantId+"_data", "Statistics", info))
    producer.close()
    */
  }
}
