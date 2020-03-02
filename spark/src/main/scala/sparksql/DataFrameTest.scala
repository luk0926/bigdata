package sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: sparksql
 * @Author: luk
 * @CreateTime: 2020/1/18 15:03
 */
object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val file: Dataset[String] = spark.read.textFile("C:\\Users\\JIGUANG\\Desktop\\word.txt")

    val split: Dataset[Array[String]] = file.map(_.split(" "))

    val personDS: Dataset[Person] = split.map(t=>Person(t(0), t(1).toInt))

    personDS.createOrReplaceTempView("v_person")

    val frame: DataFrame = spark.sql(
      """
        |select * from v_person
        |""".stripMargin)

    //将dataframe转换为dataset
    val value: Dataset[Person] = frame.as[Person]

    val s: Dataset[Int] = List(1,2,3,4,5).toDS

    spark.stop()
  }
}

case class Person(val name:String, val age:Int)
