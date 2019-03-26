package com.vita.spark.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 这是一个接收来自网络端口的信息
  * 参数 spark集群的主节点地址，网络通信的节点地址，网络通信的端口，每个多长时间作为一个单位进行执行任务
  * local[*] localhost 8888 5
  */
object Test {

  case class Person(username: String, usercount: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("hdfsTest")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    words.print()
    println()
    ssc.start()
    ssc.awaitTermination()

  }

}
