package com.github.dwiechert.chapter3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Main entry point for a simple spark application.
 * <p/>
 * To run:
 *   sbt package
 *   spark-submit --class "com.github.dwiechert.chapter3.Actions" target/scala-2.11/learning-spark_2.11-0.1.jar
 */
object Actions {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Letter Count Application")
    val sc = new SparkContext(conf)
    val input = sc.parallelize(List(1, 2, 3, 4))

    // Reduce
    val sum = input.reduce((x, y) => x + y)

    // Aggregate
    val result = input.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avg = result._1 / result._2.toDouble
    
    println(s"\n\n\nReduce produced: $sum")
    println(s"Aggregate produced: $avg\n\n\n")
  }
}
