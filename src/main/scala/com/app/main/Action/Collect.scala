/**
  * @ClassName Collect
  * @Description TODO
  * @Author zy
  * @Date 2019/5/27 16:46
  * @Version 1.0
  **/
package com.app.main.Action

import com.app.main.Transformationt.SortByKey
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 16:47 2019/5/27
 * @Param
 * @return
 **/
object Collect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(SortByKey.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list)

    val ret = listRDD.collect()



    sc.stop()
  }
}
