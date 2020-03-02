package com.kaikeba

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2020/1/8 11:40
 */
object TestPartialFunction {

  // func1是一个输入参数为Int类型，返回值为String类型的偏函数
  val func1: PartialFunction[Int, String] = {
    case 1 => "one"
    case 2 => "two"
    case 3 => "three"
    case _ => "其他"
  }

  def main(args: Array[String]): Unit = {
    val s1: String = func1(1)
    println(s1)

    val list: List[Int] = List(1, 2, 3, 4, 5, 6)
    //使用偏函数操作
    val res: List[Int] = list.filter {
      case x if x > 3 => true
      case _ => false
    }

    println(res)
  }
}
