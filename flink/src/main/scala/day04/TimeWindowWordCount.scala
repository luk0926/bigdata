package day04

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day04
 * @Author: luk
 * @CreateTime: 2020/3/24 14:05
 */
object TimeWindowWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val dataStream: DataStream[String] = env.socketTextStream("node03", 9999)

    dataStream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .process(new SumProcessFunction)
      .print()

    env.execute()
  }
}

class SumProcessFunction extends ProcessWindowFunction[(String, Int), (String, Int), Tuple, TimeWindow] {

  private val format: FastDateFormat = FastDateFormat.getInstance("HH:mm:ss")

  override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    println("当天系统时间为：" + format.format(System.currentTimeMillis()))
    println("window的处理时间为：" + format.format(context.currentProcessingTime))
    println("window的处理时间为：" + format.format(context.currentProcessingTime))
    println("window的开始时间为：" + format.format(context.window.getStart))
    println("window的结束时间为：" + format.format(context.window.getEnd))

    var sum: Int = 0
    for (eachElement <- elements) {
      sum += eachElement._2
    }

    out.collect((key.getField(0), sum))
  }
}