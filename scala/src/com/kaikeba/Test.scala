package com.kaikeba

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2020/1/3 11:49
 */
object Test {
  def main(args: Array[String]): Unit = {
    println("hello scala")

    val x: Int = 0

    val k: Int = if (x < 0) -1 else if (x == 0) 0 else 1
    println(k)

    val result: String = {
      val y = x + 10
      val z = y + "-hello"
      val m = z + "-kaikeba"
      "over"
    }
    println(result)

    println(getAddress("a")("b", "c"))

    val doubleFunc = multiply(10)
    val d: Double = doubleFunc(3)
    println(d)

  }


  def getAddress(a: String)(b: String, c: String): String = {
    return a + "-" + b + "-" + c

  }


  def multiply(x: Double) = (y: Double) => x * y
}
