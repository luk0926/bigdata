package com.kaikeba

import scala.util.Try

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2020/1/8 14:04
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    val i: Int = Try(2/0).getOrElse(10)
    println(i)
  }
}
