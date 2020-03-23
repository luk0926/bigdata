package day02.state.liststate

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.state.liststate
 * @Author: luk
 * @CreateTime: 2020/3/23 12:01
 */
object ListStateOperate {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (2L, 4d),
      (2L, 2d),
      (2L, 6d)
    )).keyBy(_._1)
        .flatMap(new CountWindowAverageWithList)
        .print()


    env.execute()
  }
}
