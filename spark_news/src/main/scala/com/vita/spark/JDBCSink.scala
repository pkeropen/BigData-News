package com.vita.spark

import java.sql.{Connection, ResultSet, SQLException, Statement}

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * 处理从StructuredStreaming中向mysql中写入数据
  */
class JDBCSink(url: String, username: String, password: String) extends ForeachWriter[Row] {

  var statement: Statement = _
  var resultSet: ResultSet = _
  var connection: Connection = _

  override def open(partitionId: Long, version: Long): Boolean = {
    connection = new MySqlPool(url, username, password).getJdbcConn()
    statement = connection.createStatement();
    print("open")
    return true
  }

  override def process(value: Row): Unit = {
    println("process step one")
    val titleName = value.getAs[String]("titleName").replaceAll("[\\[\\]]", "")
    val count = value.getAs[Long]("count")

    val querySql = "select 1 from webCount where titleName = '" + titleName + "'"
    val insertSql = "insert into webCount(titleName,count) values('" + titleName + "' , '" + count + "')"
    val updateSql = "update webCount set count = " + count + " where titleName = '" + titleName + "'"
    println("process step two")
    try {
      //查看连接是否成功
      var resultSet = statement.executeQuery(querySql)
      if (resultSet.next()) {
        println("updateSql")
        statement.executeUpdate(updateSql)
      } else {
        println("insertSql")
        statement.execute(insertSql)
      }

    } catch {
      case ex: SQLException => {
        println("SQLException")
      }
      case ex: Exception => {
        println("Exception")
      }
      case ex: RuntimeException => {
        println("RuntimeException")
      }
      case ex: Throwable => {
        println("Throwable")
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (statement == null) {
      statement.close()
    }
    if (connection == null) {
      connection.close()
    }
  }
}
