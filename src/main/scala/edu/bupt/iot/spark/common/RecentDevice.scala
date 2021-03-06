package edu.bupt.iot.spark.common

import java.util.Date

import edu.bupt.iot.util.kafka.KafkaConfig
import edu.bupt.iot.util.mysql.MysqlConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

object RecentDevice {
  def main(args: Array[String]): Unit = {
    var endTime = new Date().getTime
    var days = 1
    if(args.length > 0) endTime = args(0).toLong
    if(args.length > 1) days = args(1).toInt
    val startTime = endTime - days * 3600 * 24 * 1000
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
    val inputFiles = s"hdfs://master:9000/data/device-data-${dataFilePre}*"
    //val inputFiles = s"hdfs://master:9000/data/1527782400000"
    println(inputFiles)
    val spark = SparkSession
      .builder()
      .appName("RecentDevice")
      .master("spark://master:7077")
      .getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile(inputFiles)
      .map(_.split(","))
      .map{case Array(tenant_id, key, device_id, value, time_stamp) =>
        (tenant_id.toInt, key, device_id, value.toDouble, time_stamp.toLong)}
        .filter(item => item._5 >= startTime &&  item._5 <= endTime)
      .toDF("tenant_id", "key", "device_id", "value", "time_stamp").cache()
    data.createOrReplaceTempView("data")
    //val producer = new KafkaProducer[String, String](KafkaConfig.getProducerConf())
    val conn = MysqlConfig.getConnection()
    val tmp = spark.sql("select tenant_id, key as device_type," +
      " count(distinct device_id) as device_count, count(*) as data_count," +
      " mean(value) as mean_value, stddev(value) as stddev_value" +
      " from data" +
      " group by tenant_id, key").cache()
    tmp.createOrReplaceTempView("tmp")
    spark.sql("select tmp.tenant_id as tenant_id, tmp.device_type as device_type," +
      " concat_ws('', collect_set(tmp.device_count)) as device_count," +
      " concat_ws('', collect_set(tmp.data_count)) as data_count," +
      " count(*) as usual_data_count" +
      " from data, tmp" +
      " where tmp.tenant_id = data.tenant_id and tmp.device_type = data.key" +
      " and abs(data.value-tmp.mean_value) < 3*tmp.stddev_value" +
      " group by tmp.tenant_id, tmp.device_type")
      .map(item =>
        (item(0).toString.toInt, item(1).toString,
          item(2).toString.toInt, item(3).toString.toInt, item(4).toString.toInt,
          item(4).toString.toDouble / item(3).toString.toDouble
        ))//.toString())
      .collect()
      .foreach(item => {
        println(item)
        try {
          val insertStr = "INSERT INTO recent_device" +
            "(tenant_id, device_type, device_count, " +
            "data_count, usual_data_count, usual_data_rate, date) " +
            "VALUES(?, ?, ?, ?, ?, ?, ?)"
          val prep = conn.prepareStatement(insertStr)
          prep.setInt(1, item._1)//tenantid
          prep.setString(2, item._2)//devicetype
          prep.setInt(3, item._3)//datacount
          prep.setInt(4, item._4)//datacount
          prep.setInt(5, item._5)//usualdatacount
          prep.setDouble(6, if(item._6.isNaN) 0.0 else item._6)//rate
          prep.setDate(7, new java.sql.Date(endTime))//date
          prep.executeUpdate
        } catch{
          case e:
            Exception => e.printStackTrace
        }
        //producer.send(new ProducerRecord[String, String]("recentDevice", "recentDevice", item))
      })
    //producer.close()
    conn.close
  }
}
