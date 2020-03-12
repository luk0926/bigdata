package wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: wordcount
 * @Author: luk
 * @CreateTime: 2020/3/12 17:34
 *
 *
 *             实现一个 每隔4秒，统计最近6秒的单词计数的情况。
 */
object WindowOperatorTest {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("node03", 9999)

    val wordCountDStream: DStream[(String, Int)] = socket.flatMap(_.split(","))
      .map((_, 1))
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(6), Seconds(4))

    wordCountDStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
