package edu.bupt.iot.spark.test

import edu.bupt.iot.util.kafka.KafkaConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

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
    val inputFiles = s"hdfs://master:9000/data/${dataFilePre}*"//device-data-${dataFilePre}*"
    println(inputFiles)

    val spark = SparkSession
      .builder()
      .appName("UserDataAnalysis")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    var rawData = sc.textFile(inputFiles).map(_.split(","))

    if (!deviceId.equals("-1")) rawData = rawData.filter(item => item(2).equals(deviceId))
    if (!tenantId.equals("-1")) rawData = rawData.filter(item => item(0).equals(tenantId))
    if (!deviceType.equals("-1")) rawData = rawData.filter(item => item(1).equals(deviceType))
    val data = rawData.map(item => (item(3).toDouble, item(4).toLong))
      .filter(item => item._2 >= startTime && item._2 < endTime)
      .map(item =>  ((startTime + (item._2 - startTime) / gap * gap).toString, item._1))
      .toDF("time_stamp", "value").cache()
    data.createOrReplaceTempView("data")
    spark.sql("select time_stamp, count(*) as count, max(value) as max, min(value) as min, mean(value) as mean, stddev(value) as stddev from data group by time_stamp")
      .createOrReplaceTempView("tmp")
    val count = spark.sql("select time_stamp, count from tmp")
      .rdd.map(item => (item(0).toString, item(1)))
      .collectAsMap()
    val max = spark.sql("select time_stamp, max from tmp")
      .rdd.map(item => (item(0).toString, item(1)))
      .collectAsMap()
    val min = spark.sql("select time_stamp, min from tmp")
      .rdd.map(item => (item(0).toString, item(1)))
      .collectAsMap()
    val mean = spark.sql("select time_stamp, mean from tmp")
      .rdd.map(item => (item(0).toString, item(1)))
      .collectAsMap()
    val stddev = spark.sql("select time_stamp, stddev from tmp")
      .rdd.map(item => (item(0).toString, item(1)))
      .collectAsMap()

    val info = {
      val usualCount =  spark.sql("select data.time_stamp, count(*) as usualCount " +
        "from data, tmp " +
        "where " +
        "data.time_stamp = tmp.time_stamp and " +
        "abs(value - mean) <= 3 * stddev " +
        "group by data.time_stamp")
        .rdd.map(item => (item(0).toString, item(1)))
        .collectAsMap()
        ////////////////////
      val tmp = {
        val all  = mutable.Map[String, Any]()
        all.put("status", "success")

        val list = List[Any](
          JSONObject(max.toMap),
          JSONObject(min.toMap),
          JSONObject(mean.toMap),
          JSONObject(stddev.toMap),
          JSONObject(count.toMap),
          JSONObject(usualCount.toMap)//,
          //JSONObject(usualRate.toMap)
        )
        all.put("data", JSONArray(list))

        JSONObject(all.toMap).toString()
        }
      tmp
    }
    println(info)
    //kafka send message
    val producer = new KafkaProducer[String, String](KafkaConfig.getProducerConf())
    producer.send(new ProducerRecord[String, String](tenantId+"_data", "Statistics", info))
    producer.close()
  }
}
