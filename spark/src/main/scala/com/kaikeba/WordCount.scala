package com.kaikeba

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2020/1/14 14:03
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)

    //设置日志输出级别
    sc.setLogLevel("warn")

    val file: RDD[String] = sc.textFile("C:\\Users\\JIGUANG\\Desktop\\word.txt")
    val split: RDD[String] = file.flatMap(_.split(" "))
    val map: RDD[(String, Int)] = split.map((_,1))
    val result: RDD[(String, Int)] = map.reduceByKey(_+_)

    val sorted: RDD[(String, Int)] = result.sortBy(_._2,true)
    val tuples: Array[(String, Int)] = sorted.collect()
    tuples.foreach(println)

    sc.stop()
  }
}
