package com.github.dwiechert.chapter3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Main entry point for a simple spark application.
 * <p/>
 * To run:
 *   sbt package
 *   spark-submit --class "com.github.dwiechert.chapter3.Transformations" target/scala-2.11/learning-spark_2.11-0.1.jar
 */
object Transformations {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Letter Count Application")
    val sc = new SparkContext(conf)

    // Map
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)

    // Flatmap
    val lines = sc.parallelize(List("hello world", "hi"))
    val words = lines.flatMap(line => line.split(" "))

    println("\n\n\n" + result.collect().mkString(","))
    println(words.first() + "\n\n\n") // returns "hello"
  }
}
