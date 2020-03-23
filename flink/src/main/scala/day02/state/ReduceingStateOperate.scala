package day02.state

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.state
 * @Author: luk
 * @CreateTime: 2020/3/23 12:12
 */
object ReduceingStateOperate {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (2L, 4d),
      (2L, 2d),
      (2L, 6d)
    )).keyBy(_._1)
      .flatMap(new CountWithReduceingAverageStage)
      .print()
    env.execute()
  }
}


class CountWithReduceingAverageStage extends RichFlatMapFunction[(Long,Double),(Long,Double)]{

  private var reducingState:ReducingState[Double] = _

  override def open(parameters: Configuration): Unit = {


    val reduceSum = new ReducingStateDescriptor[Double]("reduceSum", new ReduceFunction[ Double] {
      override def reduce(value1: Double, value2: Double): Double = {
        value1+ value2

      }
    }, classOf[Double])

    reducingState = getRuntimeContext.getReducingState[Double](reduceSum)

  }
  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    reducingState.add(input._2)
    out.collect(input._1,reducingState.get())
  }
}
