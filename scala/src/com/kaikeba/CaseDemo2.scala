package com.kaikeba

import scala.util.Random

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2020/1/8 11:32
 */
object CaseDemo2 extends App {

  val arr = Array(CheckTimeOutTask, HeartBeat(10000), SubmitTask("0001", "task0001"))

  arr(Random.nextInt(arr.length)) match {
    case SubmitTask(id, name) => println(s"id=$id, name=$name")
    case HeartBeat(time) => println(s"time=$time")
    case CheckTimeOutTask => println("检查超时")
  }
}

case class SubmitTask(id: String, name: String)

case class HeartBeat(time: Long)

case object CheckTimeOutTask
