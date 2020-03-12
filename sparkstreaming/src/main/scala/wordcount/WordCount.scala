package wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage:
 * @Author: luk
 * @CreateTime: 2020/3/11 17:18
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))

    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("node03", 9999)

    val words: DStream[String] = socket.flatMap(_.split(" "))

    val pairs: DStream[(String, Int)] = words.map((_, 1))
    val workCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

    workCounts.print()

    //启动任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
