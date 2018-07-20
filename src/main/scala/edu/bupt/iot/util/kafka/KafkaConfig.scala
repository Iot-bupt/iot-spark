package edu.bupt.iot.util.kafka

import java.util.Properties

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.util.Random

object KafkaConfig {
  //10.112.233.200
  final val BOOTSTRAP_SERVERS = "kafka-service:9092"

  def getProducerConf(): Properties ={
    val p = new Properties()
    p.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS)
    p.setProperty("key.serializer", classOf[StringSerializer].getName)
    p.setProperty("value.serializer", classOf[StringSerializer].getName)
    p
  }

  def getConsumerConf() : Map[String, Object] = {
    //val kafkaParams  =
    Map[String, Object](
    "bootstrap.servers" -> BOOTSTRAP_SERVERS,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> s"consumer-group-${Random.nextInt}-${System.currentTimeMillis}",//"use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //kafkaParams
  }

}
