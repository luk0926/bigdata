package day03.windows

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day03.windows
 * @Author: luk
 * @CreateTime: 2020/3/23 18:17
 */
object TimeWindowFirst {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val dataStream: DataStream[String] = env.socketTextStream("node03", 9999)

    dataStream.flatMap(_.split(" "))
        .map((_,1))
        .keyBy(0)
        .timeWindow(Time.seconds(10), Time.seconds(5))
        .sum(1)
        .print()

    env.execute()
  }
}
