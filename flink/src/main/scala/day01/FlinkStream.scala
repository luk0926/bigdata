package day01

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01
 * @Author: luk
 * @CreateTime: 2020/3/17 15:56
 */
object FlinkStream {
  def main(args: Array[String]): Unit = {
    //程序入口类
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //从socket获取数据
    val socket: DataStream[String] = env.socketTextStream("node03", 9999)

    import org.apache.flink.api.scala._

    val resultStream: DataStream[WordCount] = socket.flatMap(_.split(" "))
      .map(t => WordCount(t, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .sum("count")

    resultStream.print()

    env.execute()
  }
}