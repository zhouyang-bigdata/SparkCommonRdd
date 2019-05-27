/**
  * @ClassName ReduceByKey
  * @Description TODO
  * @Author zy
  * @Date 2019/5/27 16:36
  * @Version 1.0
  **/
package com.app.main.Transformationt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(ReduceByKey.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    transformationOps7(sc)

    sc.stop()
  }
  /**
    * 7、reduceByKey：统计每个班级的人数
    * reduceByKey(func, [numTasks]): 在一个（K，V)对的数据集上使用，返回一个（K，V）对的数据集，
    * key相同的值，都被使用指定的reduce函数聚合到一起。和groupbykey类似，任务的个数是可以通过第二个可选参数来配置的。
    *
    * 需要注意的是还有一个reduce的操作，其为action算子，并且其返回的结果只有一个，而不是一个数据集
    * 而reduceByKey是一个transformation算子，其返回的结果是一个数据集
    */
  def transformationOps7(sc:SparkContext): Unit = {
    val list = List("hello you", "hello he", "hello me")
    val listRDD = sc.parallelize(list)
    val wordsRDD = listRDD.flatMap(line => line.split(" "))
    val pairsRDD:RDD[(String, Int)] = wordsRDD.map(word => (word, 1))
    val retRDD:RDD[(String, Int)] = pairsRDD.reduceByKey((v1, v2) => v1 + v2)

    retRDD.foreach(t => println(t._1 + "..." + t._2))
  }

//  输出结果如下：
//
//  you...1
//  hello...3
//  he...1
//  me...1

}
