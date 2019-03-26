package com.vita.spark

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{ForeachWriter, Row}

class MysqlSink(url: String, user: String, pwd: String) extends ForeachWriter[String] {

  var conn: Connection = _

  override def open(partitionId: Long, epochId: Long): Boolean = {

    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url, user, pwd)
    true
  }

  override def process(value: String): Unit = {
    val insertSql = "insert into webCount(titleName,count) values('aaaa' , '11111')"
    val p = conn.prepareStatement(insertSql)
    p.execute()
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}