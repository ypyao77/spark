package org.demo.spark.kafka

import java.text.SimpleDateFormat
import java.util.{Date, HashMap}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/*
 * Example:
 *     $ spark2-submit --queue root.hadoop.plarch --master local --deploy-mode client --class org.demo.spark.kafka.KafkaWordCountProducer target/kafka-1.0.0.jar oddev01:9092,oddev02:9092,oddev03:9092 streaming-demo 3 3
 */
object KafkaWordCountProducer {

  def getSysTime(): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: String = sdf.format(new Date())
    date
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while (true) {
      println("    " + getSysTime() + " generate next lines")

      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }
}
