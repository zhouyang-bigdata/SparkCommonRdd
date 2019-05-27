/**
  * @ClassName FlatMap
  * @Description TODO
  * @Author zy
  * @Date 2019/5/27 16:30
  * @Version 1.0
  **/
package com.app.main.Transformationt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 16:30 2019/5/27
 * @Param
 * @return
 **/
object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(FlatMap.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    transformationOps3(sc)

    sc.stop()
  }
  /**
    * 3、flatMap：将行拆分为单词
    * flatMap(func):类似于map，但是每一个输入元素，
    * 会被映射为0到多个输出元素（因此，func函数的返回值是一个Seq，而不是单一元素）
    */
  def transformationOps3(sc:SparkContext): Unit = {
    val list = List("hello you", "hello he", "hello me")
    val listRDD = sc.parallelize(list)
    val wordsRDD = listRDD.flatMap(line => line.split(" "))
    wordsRDD.foreach(println)
  }

//  输出结果如下：
//
//  hello
//  hello
//  he
//  you
//  hello
//  me



}
