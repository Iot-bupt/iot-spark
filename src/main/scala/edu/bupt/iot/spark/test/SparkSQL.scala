package edu.bupt.iot.spark.test

import org.apache.spark.sql.SparkSession

object SparkSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile("hdfs://master:9000/data_electricity/data*.csv")
      .map(_.split(","))
      .map{case Array(device_id, index_id, time_stamp, now_value, max_value, min_value) =>
        (device_id.toInt, index_id.toInt, time_stamp.toLong, now_value.toDouble, max_value.toDouble, min_value.toDouble)}
      .toDF("device_id", "index_id", "time_stamp", "now_value", "max_value", "min_value")
    data.createOrReplaceTempView("data")

    val device = spark.sparkContext.textFile("hdfs://master:9000/data_electricity/device*.csv")
      .map(_.split(",", 2))
      .map{case Array(device_id, device_name) =>
        (device_id.toInt, device_name)}
      .toDF("device_id", "device_name")
    device.createOrReplaceTempView("device")

    val index = spark.sparkContext.textFile("hdfs://master:9000/data_electricity/index*.csv")
      .map(_.split(",", 2))
      .map{case Array(index_id, index_name) =>
        (index_id.toInt, index_name)}
      .toDF("index_id", "index_name")
    index.createOrReplaceTempView("index")
    //spark.sql("select count(*) from data").show()
    spark.sql("select * from device where device_name='110kV百河铝业OptiX 155 622H(Metro1000)-64-1-OI2D-1(SDH-1)-VC4:1'").show(10)
    //spark.sql("select * from index where index_id < 20").show()
    spark.sql("select * from index, device where device.device_id = index.index_id").show()
  }
}
