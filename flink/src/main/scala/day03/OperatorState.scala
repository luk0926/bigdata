package day03

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day03
 * @Author: luk
 * @CreateTime: 2020/3/23 14:35
 *
 *              需求：实现每两条数据进行输出打印一次，不用区分数据的key
 */
object OperatorState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val sourceStream: DataStream[(String, Int)] = env.fromCollection(List(
      ("Spark", 3),
      ("Hadoop", 5),
      ("Hadoop", 7),
      ("spark", 9)
    ))

    sourceStream.addSink(new OperateTaskState).setParallelism(1)

    env.execute()
  }
}

class OperateTaskState extends SinkFunction[(String, Int)] {
  //申明列表，用于我们每两条数据打印一下
  private var listBuffer: ListBuffer[(String, Int)] = new ListBuffer[(String, Int)]

  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    listBuffer.+=(value)

    if (listBuffer.size == 2) {
      println(listBuffer)
      listBuffer.clear()
    }
  }

}