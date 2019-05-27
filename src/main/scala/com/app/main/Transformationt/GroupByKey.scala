/**
  * @ClassName GroupByKey
  * @Description TODO
  * @Author zy
  * @Date 2019/5/27 16:35
  * @Version 1.0
  **/
package com.app.main.Transformationt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 16:35 2019/5/27
 * @Param
 * @return
 **/
object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(GroupByKey.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    transformationOps6(sc)

    sc.stop()
  }
  /**
    * 6、groupByKey：对数组进行 group by key操作
    * groupByKey([numTasks]): 在一个由（K,V）对组成的数据集上调用，返回一个（K，Seq[V])对的数据集。
    * 注意：默认情况下，使用8个并行任务进行分组，你可以传入numTask可选参数，根据数据量设置不同数目的Task
    * mr中：
    * <k1, v1>--->map操作---><k2, v2>--->shuffle---><k2, [v21, v22, v23...]>---><k3, v3>
    * groupByKey类似于shuffle操作
    *
    * 和reduceByKey有点类似，但是有区别，reduceByKey有本地的规约，而groupByKey没有本地规约，所以一般情况下，
    * 尽量慎用groupByKey，如果一定要用的话，可以自定义一个groupByKey，在自定义的gbk中添加本地预聚合操作
    */
  def transformationOps6(sc:SparkContext): Unit = {
    val list = List("hello you", "hello he", "hello me")
    val listRDD = sc.parallelize(list)
    val wordsRDD = listRDD.flatMap(line => line.split(" "))
    val pairsRDD:RDD[(String, Int)] = wordsRDD.map(word => (word, 1))
    pairsRDD.foreach(println)
    val gbkRDD:RDD[(String, Iterable[Int])] = pairsRDD.groupByKey()
    println("=============================================")
    gbkRDD.foreach(t => println(t._1 + "..." + t._2))
  }


//  输出结果如下：
//
//  (hello,1)
//  (hello,1)
//  (you,1)
//  (he,1)
//  (hello,1)
//  (me,1)
//  =============================================
//  you...CompactBuffer(1)
//  hello...CompactBuffer(1, 1, 1)
//  he...CompactBuffer(1)
//  me...CompactBuffer(1)
}
