package com.kaikeba

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2020/1/14 14:16
 */
object WordCountOnSpark {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)

    //设置日志输出级别
    sc.setLogLevel("warn")

    //读取文件
    val file: RDD[String] = sc.textFile(args(0))

    val result: RDD[(String, Int)] = file.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val sorted: RDD[(String, Int)] = result.sortBy(-_._2)

    sorted.collect().foreach(t=>println(t))

    sc.stop()
  }
}
