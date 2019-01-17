package org.demo.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io
import com.fasterxml.jackson.annotation

object SparkWordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Word Count"))
    if (args.length > 1) {
        val filename = args(1)
        val lines = sc.textFile(filename)

        val words = lines.flatMap(_.split("\\s+"))
        val wordCount = words.countByValue()

        println(wordCount)
    }
  }
}
