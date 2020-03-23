package day02.state

import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.state
 * @Author: luk
 * @CreateTime: 2020/3/23 12:14
 */
object AggregrageStateOperate {
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
      .flatMap(new AggregrageState)
      .print()
    env.execute()
  }
}


class AggregrageState extends RichFlatMapFunction[(Long,Double),(Long,String)]{

  private var aggregateTotal:AggregatingState[Double, String] = _

  override def open(parameters: Configuration): Unit = {
    /**
     * name: String,
     * aggFunction: AggregateFunction[IN, ACC, OUT],
     * stateType: Class[ACC]
     */
    val aggregateStateDescriptor = new AggregatingStateDescriptor[Double, String, String]("aggregateState", new AggregateFunction[Double, String, String] {
      override def createAccumulator(): String = {
        "Contains"
      }

      override def add(value: Double, accumulator: String): String = {
        if ("Contains".equals(accumulator)) {
          accumulator + value
        }
        accumulator + "and" + value
      }

      override def getResult(accumulator: String): String = {
        accumulator
      }

      override def merge(a: String, b: String): String = {
        a + "and" + b
      }
    }, classOf[String])
    aggregateTotal = getRuntimeContext.getAggregatingState(aggregateStateDescriptor)
  }

  override def flatMap(input: (Long, Double), out: Collector[(Long, String)]): Unit = {
    aggregateTotal.add(input._2)
    out.collect(input._1,aggregateTotal.get())
  }
}
