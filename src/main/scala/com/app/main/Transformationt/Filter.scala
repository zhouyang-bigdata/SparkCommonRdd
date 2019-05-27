package com.app.main.Transformationt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 16:12 2019/5/27
 * @Param
 * @return
 **/
object Filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(Filter.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    transformationOps2(sc)

    sc.stop()
  }
  /**
    * 2、filter：过滤出集合中的奇数
    * filter(func): 返回一个新的数据集，由经过func函数后返回值为true的原元素组成
    *
    * 一般在filter操作之后都要做重新分区（因为可能数据量减少了很多）
    */
  def transformationOps2(sc:SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val listRDD = sc.parallelize(list)
    val retRDD = listRDD.filter(num => num % 2 == 0)
    retRDD.foreach(println)
  }


//  输出结果如下：
//
//  6
//  2
//  8
//  4
//  10
}
