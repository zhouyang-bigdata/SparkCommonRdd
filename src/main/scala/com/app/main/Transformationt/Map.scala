package com.app.main.Transformationt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description TODO
 * @Date 12:31 2019/5/27
 * @Param
 * @return
 **/
object Map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(Map.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    transformationOps1(sc)

    sc.stop()
  }
  /**
    * 1、map：将集合中每个元素乘以7
    * map(func):返回一个新的分布式数据集，由每个原元素经过func函数转换后组成
    */
  def transformationOps1(sc:SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val listRDD = sc.parallelize(list)
    val retRDD = listRDD.map(num => num * 7)
    retRDD.foreach(num => println(num))
  }

//  执行结果如下：
//
//  42
//  7
//  49
//  14
//  56
//  21
//  63
//  28
//  70
//  35
}
