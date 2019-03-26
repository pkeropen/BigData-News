package com.vita.spark.utils

import java.io.PrintWriter
import java.net.ServerSocket

class LoggerSimulation {

}

object LoggerSimulation {

  var numIndex = 0

  /**
    * 生成一个字母
    *
    * @param 字母的下标
    * @return 生成的字母
    */
  def gennerateContent(index: Int): String = {
    import scala.collection.mutable.ListBuffer
    val charList = ListBuffer[Char]();
    for (i <- 65 to 90) {
      charList += i.toChar
    }
    val charArray = charList.toArray
    charArray(index).toString();
  }

  def gennerateNumber(): String = {
    //    numIndex += 1
    //    return numIndex.toString
    return "a,b,c,d,e,f"
  }

  /**
    * 生成随机下标
    *
    * @return 返回一个下标
    */
  def index = {
    import java.util.Random
    val rdm = new Random()
    rdm.nextInt(7)
  }

  /**
    * 启动一个main方法来创建一个serversockt发送消息
    *
    * @param args 端口，发送的时间间隔
    */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage:<port><millisecond>")
      System.exit(1);
    }

    val listener = new ServerSocket(args(0).toInt)
    println("已经做好连接的准备-------")
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("Got client connected from:" + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream, true)
          while (true) {
            Thread.sleep(args(1).toLong)
            //            val content = gennerateContent(index)
            val content = gennerateNumber()
            println(content)
            out.write(content + "\n")
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
