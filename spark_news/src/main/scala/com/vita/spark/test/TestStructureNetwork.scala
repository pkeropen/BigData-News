package com.vita.spark.test

import org.apache.spark.sql.SparkSession

object TestStructureNetwork {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestStructureNetwork").master("local[2]").getOrCreate()

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
