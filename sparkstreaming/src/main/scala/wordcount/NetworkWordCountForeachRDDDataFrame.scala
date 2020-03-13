package wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: wordcount
 * @Author: luk
 * @CreateTime: 2020/3/13 17:24
 */
object NetworkWordCountForeachRDDDataFrame {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node03", 9999)

    val words: DStream[String] = dstream.flatMap(_.split(","))

    words.foreachRDD(rdd=>{
      val spark: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val wordDataFrame: DataFrame = rdd.toDF("word")
      wordDataFrame.createOrReplaceTempView("v_words")

      val wordCountsDataFrame: DataFrame = spark.sql("select word, count(*) as total from v_words group by word")
      wordCountsDataFrame.show()

    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
