package org.demo.spark.kafka.security

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
  * <zkQuorum> is a list of one or more zookeeper servers that make quorum
  * <group> is the name of kafka consumer group
  * <topics> is a list of one or more kafka topics to consume from
  * <numThreads> is the number of threads the kafka consumer should use
  *
  * Example:
  * `$ bin/run-example \
  *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
  * my-consumer-group topic1,topic2 1`
  *
  *    e.g.
  * $ spark2-submit --queue root.hadoop.plarch --master local --deploy-mode client --files /data/security/arch_onedata/jaas_kafka.conf,/data/security/arch_onedata/arch_onedata.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/data/security/arch_onedata/jaas_kafka.conf" --driver-java-options "-Djava.security.auth.login.config=/data/security/arch_onedata/jaas_kafka.conf" --class org.demo.spark.kafka.security.KafkaWordCount target/kafka-1.0.0.jar arch-od-data03:2181,arch-od-data04:2181,arch-od-data05:2181/kafka streaming-demo mygroup 3
  */
object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <topics> <group> <numThreads>")
      System.exit(1)
    }

    // StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, topics, group, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.allowMultipleContexts", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.checkpoint("checkpoint")

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "zookeeper.connection.timeout.ms" -> "10000",
      "security.protocol" -> "SASL_PLAINTEXT"
    )

    val messages = KafkaUtils.createStream(ssc, kafkaParams, topicMap, StorageLevel.MEMORY_ONLY_SER)
    messages.print()
    // .map(_._2)
    // lines.print()
    /*
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()
    */

    ssc.start()
    ssc.awaitTermination()
  }
}
