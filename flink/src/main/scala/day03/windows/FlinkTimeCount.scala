package day03.windows

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day03.windows
 * @Author: luk
 * @CreateTime: 2020/3/23 18:32
 */
object FlinkTimeCount {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val socketStream: DataStream[String] = environment.socketTextStream("node01",9000)
    val print: DataStreamSink[(Int, Int)] = socketStream
      .map(x => (1, x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce(new ReduceFunction[(Int, Int)] {
        override def reduce(t: (Int, Int), t1: (Int, Int)): (Int, Int) = {
          (t._1, t._2 + t1._2)
        }
      }).print()

    environment.execute("startRunning")
  }
}
