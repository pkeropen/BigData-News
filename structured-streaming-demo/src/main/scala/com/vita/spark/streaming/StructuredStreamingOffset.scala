package com.vita.spark.streaming

import com.vita.Constants
import com.vita.redies.RedisSingle
import com.vita.spark.streaming.writer.RedisWriteKafkaOffset
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

/**
  * StructuredStreaming
  * 记录kafka上一次的Offset，从之前的Offset继续消费
  */
object StructuredStreamingOffset {

  val LOGGER: Logger = LogManager.getLogger("StructuredStreamingOffset")

  //topic
  val SUBSCRIBE = "log"

  case class readLogs(context: String, offset: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("StructuredStreamingOffset")
      .getOrCreate()

    //开始 offset
    var startOffset = -1

    //init
    val redisSingle: RedisSingle = new RedisSingle()
    redisSingle.init(Constants.IP, Constants.PORT)
    //get redis
    if (redisSingle.exists(Constants.REDIDS_KEY) && redisSingle.getTime(Constants.REDIDS_KEY) != -1) {
      startOffset = redisSingle.get(Constants.REDIDS_KEY).toInt
    }

    //sink
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", SUBSCRIBE)
      .option("startingOffsets", "{\"" + SUBSCRIBE + "\":{\"0\":" + startOffset + "}}")
      .load()

    import spark.implicits._

    //row 包含: key、value 、topic、 partition、offset、timestamp、timestampType
    val lines = df.selectExpr("CAST(value AS STRING)", "CAST(offset AS LONG)").as[(String, Long)]

    val content = lines.map(x => readLogs(x._1, x._2.toString))

    val count = content.toDF("context", "offset")

    //sink foreach 记录offset
    val query = count
      .writeStream
      .foreach(new RedisWriteKafkaOffset)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
