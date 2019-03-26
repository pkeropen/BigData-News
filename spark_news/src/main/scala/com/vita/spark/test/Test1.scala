package com.vita.spark.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("TransformationOperator")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[String] = List("张无忌", "赵敏", "周芷若")
    val rdd: RDD[String] = sc.parallelize(list)


    val list1: List[(Int, String)] = List((1, "东方不败"), (2, "令狐冲"), (3, "林平之"))
    val list2: List[(Int, Int)] = List((1, 99), (2, 98), (3, 97))

    val rdd1: RDD[(Int, String)] = sc.parallelize(list1)
    val rdd2: RDD[(Int, Int)] = sc.parallelize(list2)
    rdd1.join(rdd2).foreach(x => println("学号： " + x._1 + "名字：" + x._2._1 + " 分数：" + x._2._2))

  }
}
