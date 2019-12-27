package com.knoldus

import java.util
import java.util.Properties
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

object Consumer extends App {
  val props = new Properties()

  props.put(ConsumerConfig.CLIENT_ID_CONFIG, "2")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "null")
  props.put(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
    "localhost:9092,localhost:9093,localhost:9094"
  )
  props.put(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    classOf[StringDeserializer]
  )
  props.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    classOf[StringDeserializer]
  )
  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Arrays.asList("SinkTopic1"))
  try {
    while (true) {
      val records = consumer.poll(1000).asScala.iterator
      for (value <- records)
        println(value.key(), value.value(), value.offset(), value.partition())
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    //consumer.close()
  }
}
