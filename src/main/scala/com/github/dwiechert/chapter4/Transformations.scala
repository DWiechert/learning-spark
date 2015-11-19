package com.github.dwiechert.chapter4

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Main entry point for a simple spark application.
 * <p/>
 * To run:
 *   sbt package
 *   spark-submit --class "com.github.dwiechert.chapter4.Transformations" target/scala-2.11/learning-spark_2.11-0.1.jar
 */
object Transformations {
  def main(args: Array[String]) {
    val logFile = System.getenv("SPARK_HOME") + "/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Letter Count Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val pairs = logData.map(x => (x.split(" ")(0), x))

    // Filter
    val shortPairs = pairs.filter{case (key, value) => value.length < 20}.count()
    println(s"\n\n\nNumber of lines less than 20 characters:$shortPairs\n\n\n")

    // ReduceByKey
    val words = logData.flatMap(x => x.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    println(s"\n\n\nCounted the number of times each word appears:\n$wordCount\n\n\n")

    // CountByKey
    val result = wordCount.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
    println("\n\n\nCombineByKey resulted in:\n")
    result.collectAsMap().foreach(println(_))
    println("\n\n\n")
  }
}
