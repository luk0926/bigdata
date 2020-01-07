package com.kaikeba

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2020/1/7 15:14
 */
object Person {

  def apply(name:String, age:Int): Person = new Person(name, age)

  def apply(name: String): Person = new Person(name, 20)

  def apply(age: Int): Person = new Person("xxx", age)

  def apply(): Person = new Person("xxx", 10)
}

class Person (var name:String, var age:Int){
  override def toString: String = s"Person($name,$age)"
}


object Main2 {
  def main(args: Array[String]): Unit = {
    val p1 = Person("张三", 10)
    println(p1)

  }
}