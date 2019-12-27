package com.knoldus

import java.util.Properties

import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.serialization.StringSerializer

object Producer extends App {
  val props: Properties = new Properties()
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "2")
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer]
  )
  props.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer]
  )

  val producer = new KafkaProducer[String, String](props)
  val topic = "SourceTopic1"
  try {

    val record =
      new ProducerRecord[String, String](
        topic,
        "dev1",
        "Hii i am swapnil how are you i am fine "
      )
    val metadata = producer.send(record)
    printf(
      s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
      record.key(),
      record.value(),
      metadata.get().partition(),
      metadata.get().offset()
    )
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
