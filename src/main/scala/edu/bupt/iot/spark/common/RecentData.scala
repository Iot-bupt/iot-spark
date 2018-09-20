package edu.bupt.iot.spark.common

import org.apache.spark.sql.SparkSession
import java.util.Date

import edu.bupt.iot.util.kafka.KafkaConfig
import edu.bupt.iot.util.mysql.MysqlConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.control.Breaks

object RecentData {
  def main(args: Array[String]): Unit = {
    var endTime = new Date().getTime
    if(args.length > 0) endTime = args(0).toLong
    val startTime = endTime - 3600 * 24 * 1000
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
    //val inputFiles = s"hdfs://master:9000/data/1525428000000"
    println(inputFiles)
    val spark = SparkSession
      .builder()
      .appName("RecentData")
      .master("spark://master:7077")
      .getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile(inputFiles)
      .map(_.split(","))
      .map{case Array(tenant_id, key, device_id, value, time_stamp) =>
        (tenant_id.toInt, key, device_id, value.toDouble, time_stamp.toLong)}
      .filter(item => item._5 >= startTime &&  item._5 <= endTime)
      //.filter(item => item._4 >= startTime && item._4 < endTime)
      .toDF("tenant_id", "key", "device_id", "value", "time_stamp")
    data.createOrReplaceTempView("data")
    //println(data.show(10))
    ///val producer = new KafkaProducer[String, String](KafkaConfig.getProducerConf())
    val conn = MysqlConfig.getConnection()
    val tmp = spark.sql("select tenant_id, key as device_type," +
      " max(value) as max_value, min(value) as min_value," +
      " mean(value) as mean_value, stddev(value) as stddev_value, " +
      " count(*) as data_count" +
      " from data" +
      " group by tenant_id, key").cache()
    tmp.createOrReplaceTempView("tmp")
    spark.sql("select tmp.tenant_id as tenant_id, tmp.device_type as device_type," +
      " concat_ws('', collect_set(tmp.max_value)) as max_value," +
      " concat_ws('', collect_set(tmp.min_value)) as min_value," +
      " concat_ws('', collect_set(tmp.mean_value)) as mean_value," +
      " concat_ws('', collect_set(tmp.stddev_value)) as stddev_value," +
      " concat_ws('', collect_set(tmp.data_count)) as data_count," +
      " count(*) as usual_data_count" +
      " from data, tmp" +
      " where tmp.tenant_id = data.tenant_id and tmp.device_type = data.key" +
      " and abs(data.value-tmp.mean_value) < 3*tmp.stddev_value" +
      " group by tmp.tenant_id, tmp.device_type")
      .map(item =>
        (item(0).toString.toInt, item(1).toString,
          item(2).toString.toDouble, item(3).toString.toDouble,
          item(4).toString.toDouble, item(5).toString.toDouble,
          item(6).toString.toInt, item(7).toString.toInt,
          item(7).toString.toDouble / item(6).toString.toDouble))//.toString())
      .collect()
      .foreach(item => {
        println(item)
        try {
          val insertStr = "INSERT INTO recent_data" +
            "(tenant_id, device_type, max_value, min_value, mean_value, stddev_value, " +
            "data_count, usual_data_count, usual_data_rate, date) " +
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
          val prep = conn.prepareStatement(insertStr)
          prep.setInt(1, item._1)//tenantid
          prep.setString(2, item._2)//devicetype
          prep.setDouble(3, if (item._3.isNaN) 0.0 else item._3)//max
          prep.setDouble(4, if (item._4.isNaN) 0.0 else item._4)//min
          prep.setDouble(5, if (item._5.isNaN) 0.0 else item._5 )//mean
          prep.setDouble(6, if (item._6.isNaN) 0.0 else item._6 )//stddev
          prep.setInt(7, item._7)//datacount
          prep.setInt(8, item._8)//usualdatacount
          prep.setDouble(9, if (item._9.isNaN) 0.0 else item._9)//rate
          prep.setDate(10, new java.sql.Date(endTime))//date
          println(prep.toString)
          prep.executeUpdate
        } catch{
          case e:
            Exception => e.printStackTrace
        }
        //producer.send(new ProducerRecord[String, String]("recentData", "recentData", item))
      })
    //producer.close()
    conn.close
  }
}
