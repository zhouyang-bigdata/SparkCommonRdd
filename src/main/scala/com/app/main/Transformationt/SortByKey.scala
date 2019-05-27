/**
  * @ClassName SortByKey
  * @Description TODO
  * @Author zy
  * @Date 2019/5/27 16:42
  * @Version 1.0
  **/
package com.app.main.Transformationt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(SortByKey.getClass.getSimpleName)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    transformationOps9(sc)

    sc.stop()
  }
  /**
    * sortByKey：将学生身高进行（降序）排序
    *     身高相等，按照年龄排（升序）
    */
  def transformationOps9(sc: SparkContext): Unit = {
    val list = List(
      "1,李  磊,22,175",
      "2,刘银鹏,23,175",
      "3,齐彦鹏,22,180",
      "4,杨  柳,22,168",
      "5,敦  鹏,20,175"
    )
    val listRDD:RDD[String] = sc.parallelize(list)

    /*  // 使用sortBy操作完成排序
    val retRDD:RDD[String] = listRDD.sortBy(line => line, numPartitions = 1)(new Ordering[String] {
        override def compare(x: String, y: String): Int = {
            val xFields = x.split(",")
            val yFields = y.split(",")
            val xHgiht = xFields(3).toFloat
            val yHgiht = yFields(3).toFloat
            val xAge = xFields(2).toFloat
            val yAge = yFields(2).toFloat
            var ret = yHgiht.compareTo(xHgiht)
            if (ret == 0) {
                ret = xAge.compareTo(yAge)
            }
            ret
        }
    } ,ClassTag.Object.asInstanceOf[ClassTag[String]])
    */
    // 使用sortByKey完成操作,只做身高降序排序
    val heightRDD:RDD[(String, String)] = listRDD.map(line => {
      val fields = line.split(",")
      (fields(3), line)
    })
    val retRDD:RDD[(String, String)] = heightRDD.sortByKey(ascending = false, numPartitions = 1)   // 需要设置1个分区，否则只是各分区内有序
    retRDD.foreach(println)

    // 使用sortByKey如何实现sortBy的二次排序？将上面的信息写成一个java对象，然后重写compareTo方法，在做map时，key就为该对象本身，而value可以为null

  }



//  输出结果如下：
//
//  (180,3,齐彦鹏,22,180)
//  (175,1,李  磊,22,175)
//  (175,2,刘银鹏,23,175)
//  (175,5,敦  鹏,20,175)
//  (168,4,杨  柳,22,168)
}
