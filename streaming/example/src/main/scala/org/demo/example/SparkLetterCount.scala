package org.demo.example

import org.apache.spark.{SparkConf, SparkContext}

object SparkLetterCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val threshold = if (args.length > 1) args(1).toInt else 1

    // split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // count characters
    val charCounts = wordCounts.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _).filter(_._2 >= threshold)

    System.out.println(charCounts.collect().mkString(", "))
  }
}
