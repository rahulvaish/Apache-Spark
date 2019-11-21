package com.sparkscala


import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)//"/home/rahul/IdeaProjects/SparkScalaMavenApp/src/main/resource/samplesource.txt"

   // val outputPath = args(1)
    val sc = SparkSession.builder().appName("SampleApp").master("local").getOrCreate().sparkContext
    val lines = sc.textFile(inputPath)
    val wordCounts = lines.flatMap {line => line.split(" ")}
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    //wordCounts.saveAsTextFile(outputPath)
    wordCounts.collect().foreach(println)
  }
}
