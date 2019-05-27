/**
  * @ClassName SaveAsNewAPIHadoopFile
  * @Description TODO
  * @Author zy
  * @Date 2019/5/27 16:52
  * @Version 1.0
  **/
package com.app.main.Action

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO 
 * @Date 16:53 2019/5/27
 * @Param 
 * @return  
 **/
/**
  * Spark算子操作之Action
  *     saveAsNewAPIHAdoopFile
  *     * saveAsHadoopFile
  * 和saveAsNewAPIHadoopFile的唯一区别就在于OutputFormat的不同
  * saveAsHadoopFile的OutputFormat使用的：org.apache.hadoop.mapred中的早期的类
  * saveAsNewAPIHadoopFile的OutputFormat使用的：org.apache.hadoop.mapreduce中的新的类
  * 使用哪一个都可以完成工作
  *
  * 前面在使用saveAsTextFile时也可以保存到hadoop文件系统中，注意其源代码也是使用上面的操作的
  *
  *   Caused by: java.net.UnknownHostException: ns1
    ... 35 more
  找不到ns1，因为我们在本地没有配置，无法正常解析，就需要将hadoop的配置文件信息给我们加载进来
    hdfs-site.xml.heihei,core-site.xml.heihei
  */

object SaveAsNewAPIHadoopFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(SaveAsNewAPIHadoopFile.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val list = List("hello you", "hello he", "hello me")
    val listRDD = sc.parallelize(list)
    val pairsRDD = listRDD.map(word => (word, 1))
    val retRDD = pairsRDD.reduceByKey((v1, v2) => v1 + v2)

    retRDD.saveAsNewAPIHadoopFile(
      "hdfs://ns1/spark/action",      // 保存的路径
      classOf[Text],                      // 相当于mr中的k3
      classOf[IntWritable],               // 相当于mr中的v3
      classOf[TextOutputFormat[Text, IntWritable]]    // 设置(k3, v3)的outputFormatClass
    )

  }

}
