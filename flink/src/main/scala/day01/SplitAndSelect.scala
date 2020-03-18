package day01

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01
 * @Author: luk
 * @CreateTime: 2020/3/18 11:28
 */
object SplitAndSelect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val firstStream: DataStream[String] = env.fromCollection(Array("hello world","spark flink"))

    val splitStream: SplitStream[String] = firstStream.split(new OutputSelector[String] {
      override def select(value: String): lang.Iterable[String] = {
        val seclectorNameList: util.ArrayList[String] = new util.ArrayList[String]()

        if (value.contains("hello")) {
          seclectorNameList.add("hello_selector")
        } else {
          seclectorNameList.add("other")
        }

        seclectorNameList
      }
    })

    splitStream.select("hello_selector").print()

    env.execute()
  }
}
