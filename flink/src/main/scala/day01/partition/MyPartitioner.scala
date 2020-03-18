package day01.partition

import org.apache.flink.api.common.functions.Partitioner

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.partition
 * @Author: luk
 * @CreateTime: 2020/3/18 14:01
 */
class MyPartitioner extends Partitioner[String]{
  override def partition(k: String, i: Int): Int = {
    println("分区数为：" + i)

    if (k.contains("hello")){
      0
    }else {
      1
    }
  }
}
