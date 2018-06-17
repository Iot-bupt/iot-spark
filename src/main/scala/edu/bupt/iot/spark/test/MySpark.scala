package edu.bupt.iot.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object MySpark {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("mySpark")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6)).map(_+5)
    val mappedRDD = rdd.filter(_>10).collect()
    println(rdd.reduce(_+_))
    for (arg <- mappedRDD)
      print(arg+" ")
    mappedRDD.foreach(println(_))
    println()
    println("math is work")
  }
}
