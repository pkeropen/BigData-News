package com.vita.spark

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * 结构化流从kafka中读取数据存储到关系型数据库mysql
  * 目前结构化流对kafka的要求版本0.10及以上
  */
object StructuredStreamingKafka {

  case class Weblog(datatime: String,
                    userid: String,
                    searchname: String,
                    retorder: String,
                    cliorder: String,
                    cliurl: String)

  val LOGGER: Logger = LogManager.getLogger("vita")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("streaming")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
//      .option("kafka.bootstrap.servers", "hadoop01:9092")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
//        val df = spark.readStream
//          .format("socket")
//          .option("host", "localhost")
//          .option("port", 9998)
//          .load()

    import spark.implicits._

    val lines = df.selectExpr("CAST(value AS STRING)").as[String]
    //    //    lines.map(_.split(",")).foreach(x => print(" 0 = " + x(0) + " 1 = " + x(1) + " 2 = " + x(2) + " 3 = " + x(3) + " 4 = " + x(4) + " 5 = " + x(5)))

    val weblog = lines.map(_.split(","))
      .map(x => Weblog(x(0), x(1), x(2), x(3), x(4), x(5)))

    val titleCount = weblog
      .groupBy("searchname")
      .count()
      .toDF("titleName", "count")

    val url = "jdbc:mysql://hadoop01:3306/test"
    val username = "root"
    val password = "root"

    val writer = new JDBCSink(url, username, password)
    //        val writer = new MysqlSink(url, username, password)

//    val query = titleCount
//      .writeStream
//      .foreach(writer)
//      .outputMode("update")
//      .trigger(ProcessingTime("5 seconds"))
//      .start()

    val query = titleCount
      .writeStream
      .outputMode("update")
      .trigger(ProcessingTime("5 seconds"))
      .format("console")
        .start()

    query.awaitTermination()
  }
}
