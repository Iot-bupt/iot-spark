package edu.bupt.iot.util.kafka

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class ConsumerRecordRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(Class.forName("org.apache.kafka.clients.consumer.ConsumerRecord"))
  }
}
