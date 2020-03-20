package day02.state

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.state
 * @Author: luk
 * @CreateTime: 2020/3/20 16:04
 */
object ValueStateOperate {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (1L, 4d),
      (1L, 2d)
    )).keyBy(_._1)
        .flatMap(new CountWindowAverage())
        .print()


    env.execute()
  }
}
