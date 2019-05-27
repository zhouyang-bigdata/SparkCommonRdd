/**
  * @ClassName Sample
  * @Description TODO
  * @Author zy
  * @Date 2019/5/27 16:31
  * @Version 1.0
  **/
package com.app.main.Transformationt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 16:32 2019/5/27
 * @Param
 * @return
 **/
object Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(Sample.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    transformationOps4(sc)

    sc.stop()
  }
  /**
    * 4、sample：根据给定的随机种子seed，随机抽样出数量为frac的数据
    * sample(withReplacement, frac, seed): 根据给定的随机种子seed，随机抽样出数量为frac的数据
    * 抽样的目的：就是以样本评估整体
    * withReplacement:
    *     true：有放回的抽样
    *     false：无放回的抽样
    * frac：就是样本空间的大小，以百分比小数的形式出现，比如20%，就是0.2
    *
    * 使用sample算子计算出来的结果可能不是很准确，1000个数，20%，样本数量在200个左右，不一定为200
    *
    * 一般情况下，使用sample算子在做spark优化（数据倾斜）的方面应用最广泛
    */
  def transformationOps4(sc:SparkContext): Unit = {
    val list = 1 to 1000
    val listRDD = sc.parallelize(list)
    val sampleRDD = listRDD.sample(false, 0.2)

    sampleRDD.foreach(num => print(num + " "))
    println
    println("sampleRDD count: " + sampleRDD.count())
    println("Another sampleRDD count: " + sc.parallelize(list).sample(false, 0.2).count())
  }

//  输出结果如下：
//
//  sampleRDD count: 219
//  Another sampleRDD count: 203



}
