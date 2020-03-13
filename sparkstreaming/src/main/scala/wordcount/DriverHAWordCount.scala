package wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: wordcount
 * @Author: luk
 * @CreateTime: 2020/3/13 16:22
 */
object DriverHAWordCount {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val checkpointDirectory:String="hdfs://node01:8020/tmp/checkpointDir";

  def main(args: Array[String]): Unit = {
    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc,Seconds(2))
      ssc.checkpoint(checkpointDirectory)

      val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node03",9999)

      val wordCountDStream = dstream.flatMap(_.split(","))
        .map((_, 1))
        .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
          val currentCount = values.sum
          val lastCount = state.getOrElse(0)
          Some(currentCount + lastCount)
        })

      wordCountDStream.print()

      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
