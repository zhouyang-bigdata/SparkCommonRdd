/**
  * @ClassName Union
  * @Description TODO
  * @Author zy
  * @Date 2019/5/27 16:33
  * @Version 1.0
  **/
package com.app.main.Transformationt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 16:33 2019/5/27
 * @Param
 * @return
 **/
object Union {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(Union.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    transformationOps5(sc)

    sc.stop()
  }
  /**
    * 5、union：返回一个新的数据集，由原数据集和参数联合而成
    * union(otherDataset): 返回一个新的数据集，由原数据集和参数联合而成
    * 类似数学中的并集，就是sql中的union操作，将两个集合的所有元素整合在一块，包括重复元素
    */
  def transformationOps5(sc:SparkContext): Unit = {
    val list1 = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val list2 = List(7, 8, 9, 10, 11, 12)
    val listRDD1 = sc.parallelize(list1)
    val listRDD2 = sc.parallelize(list2)
    val unionRDD = listRDD1.union(listRDD2)

    unionRDD.foreach(println)
  }

//  输出结果如下：
//
//  1
//  6
//  2
//  7
//  3
//  8
//  4
//  9
//  5
//  10
//  7
//  8
//  9
//  10
//  11
//  12

}
