package org.demo.spark.kafka.security

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import org.apache.spark.internal.Logging
/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  * <brokers> is a list of one or more Kafka brokers
  * <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  * $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  * topic1,topic2
  *
  * e.g.
  *     $ spark2-submit --queue root.hadoop.plarch --master local --deploy-mode client  --files /data/security/arch_onedata/jaas_kafka.conf,/data/security/arch_onedata/arch_onedata.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/data/security/arch_onedata/jaas_kafka.conf" --driver-java-options "-Djava.security.auth.login.config=/data/security/arch_onedata/jaas_kafka.conf" --class org.demo.spark.kafka.security.DirectKafkaWordCount target/kafka-1.0.0.jar arch-od-data03:9092,arch-od-data04:9092,arch-od-data05:9092 streaming-demo
  */
object DirectKafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    // StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id"-> "test2",
      "security.protocol" -> "SASL_PLAINTEXT",
      "auto.offset.reset" -> "smallest"
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topics))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

// scalastyle:on println