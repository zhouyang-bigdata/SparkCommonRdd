/**
  * @ClassName Join
  * @Description TODO
  * @Author zy
  * @Date 2019/5/27 16:38
  * @Version 1.0
  **/
package com.app.main.Transformationt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 16:38 2019/5/27
 * @Param
 * @return
 **/
object Join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(Join.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    val infoPairRDD = sc.parallelize(Seq((1,"leaf"),(2,"xpleaf"),(3,"yyh")))
    val scorePairRDD = sc.parallelize(Seq((1, 93), (2, 91), (3, 86), (4, 97)))
    val joinedRDD = infoPairRDD.join(scorePairRDD)
    joinedRDD.foreach(println)

    sc.stop()
  }


}
