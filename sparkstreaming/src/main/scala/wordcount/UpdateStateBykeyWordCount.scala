package wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage:
 * @Author: luk
 * @CreateTime: 2020/3/12 13:54
 */
object UpdateStateBykeyWordCount {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))

    ssc.checkpoint("hdfs://node01:8020/tmp/checkpointDir")

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node03", 9999)

    val wordCountDStream: DStream[(String, Int)] = dstream.flatMap(_.split(","))
      .map((_, 1))
      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        val currentSum: Int = values.sum
        val lastCount: Int = state.getOrElse(0)
        Some(currentSum + lastCount)
      })

    wordCountDStream.print()


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
