package com.knoldus

import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.KStreamBuilder

object HelloStreamApp extends App {
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Hello")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(
    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
    Serdes.String().getClass
  )
  props.put(
    StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
    Serdes.String.getClass
  )
  val builder = new KStreamBuilder()
  val sourceStream = builder.stream("SourceTopic1")
  sourceStream.to("SinkTopic1")
  val stream = new KafkaStreams(builder, props)
  stream.start()
}
