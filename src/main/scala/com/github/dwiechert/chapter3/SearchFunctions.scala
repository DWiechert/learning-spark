package com.github.dwiechert.chapter3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class SearchFunctions(val query: String) {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchesFunctionReference(rdd: RDD[String]): RDD[Boolean] = {
    // Problem: "isMatch" means "this.isMatch", so we pass all of "this"
    rdd.map(isMatch)
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[Array[String]] = {
    // Problem: "query" means "this.query", so we pass all of "this"
    rdd.map(x => x.split(query))
  }

  def getMatchesNoReference(rdd: RDD[String]): RDD[Array[String]] = {
    // Safe: extract just the field we need into a local variable
    val query_ = this.queryrdd.map(x => x.split(query_))
  }
}
