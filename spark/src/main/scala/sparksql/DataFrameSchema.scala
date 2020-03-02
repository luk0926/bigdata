package sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: sparksql
 * @Author: luk
 * @CreateTime: 2020/1/18 16:40
 */
object DataFrameSchema {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val file: RDD[String] = sc.textFile("C:\\Users\\JIGUANG\\Desktop\\word.txt")

    val split: RDD[Array[String]] = file.map(_.split(" "))
    val rdd: RDD[Row] = split.map(t=>Row(t(0), t(1).toInt))

    val schema: StructType = StructType(
        StructField("name", StringType, true) ::
        StructField("age", IntegerType, true) :: Nil
    )

    val df: DataFrame = spark.createDataFrame(rdd, schema)

    df.printSchema()
    df.show()

    sc.stop()
    spark.stop()
  }
}
