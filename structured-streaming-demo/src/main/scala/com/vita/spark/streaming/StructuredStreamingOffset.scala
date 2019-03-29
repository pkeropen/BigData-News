package com.vita.spark.streaming

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredStreamingOffset {

  val LOGGER: Logger = LogManager.getLogger(StructuredStreamingOffset.getClass.getSimpleName)

  case class readLogs(title: String)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(StructuredStreamingOffset.getClass.getSimpleName)
      .getOrCreate()

    def df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    import spark.implicits._

    val lines = df.selectExpr("CAST(key AS STRING)").as[(String)]

    val content = lines
      .map(_.split(","))
      .map(x => readLogs(x(0)))

    val count = content.groupBy("title")
      .count()
      .toDF()

    val query = count
      .writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .start()

    query.awaitTermination()
  }

}
