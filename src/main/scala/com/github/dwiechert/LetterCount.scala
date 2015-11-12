package com.github.dwiechert

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Main entry point for a simple spark application.
 * <p/>
 * To run:
 *   sbt package
 *   spark-submit --class "com.github.dwiechert.LetterCount" target/scala-2.11/learning-spark_2.11-0.1.jar
 */
object LetterCount {
  def main(args: Array[String]) {
    val logFile = System.getenv("SPARK_HOME") + "/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Letter Count Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"\n\n\nLines with a: $numAs, Lines with b: $numBs\n\n\n")
  }
}
