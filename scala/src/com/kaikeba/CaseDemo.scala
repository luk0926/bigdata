package com.kaikeba

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2020/1/8 11:09
 */
object CaseDemo {
  def main(args: Array[String]): Unit = {
    val p1: CasePerson = new CasePerson("a", 10)
    println(p1)

    val p2: CasePerson = CasePerson("b", 20)
    println(p2)

  }

}

// 定义一个样例类
// 样例类有两个成员name、age
case class CasePerson(var name:String, var age:Int)