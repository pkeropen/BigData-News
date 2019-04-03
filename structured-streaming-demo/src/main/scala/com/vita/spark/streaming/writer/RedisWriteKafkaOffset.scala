package com.vita.spark.streaming.writer

import com.vita.Constants
import com.vita.redies.RedisSingle
import org.apache.spark.sql.{ForeachWriter, Row}

class RedisWriteKafkaOffset extends ForeachWriter[Row] {
  var redisSingle: RedisSingle = _

  override def open(partitionId: Long, version: Long): Boolean = {
    redisSingle = new RedisSingle()
    redisSingle.init(Constants.IP, Constants.PORT)
    true
  }

  override def process(value: Row): Unit = {
    val offset = value.getAs[String]("offset")
    redisSingle.set(Constants.REDIDS_KEY, offset)
  }

  override def close(errorOrNull: Throwable): Unit = {
    redisSingle.getJedis().close()
    redisSingle.getPool().close()
  }
}
