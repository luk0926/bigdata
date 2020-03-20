package day02.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.state
 * @Author: luk
 * @CreateTime: 2020/3/20 16:07
 */
class CountWindowAverage extends RichFlatMapFunction[(Long, Double), (Long, Double)] {

  private var sum: ValueState[(Long, Double)] = _

  override def flatMap(in: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    // access the state value
    val tmpCurrentSum: (Long, Double) = sum.value()

    // If it hasn't been used before, it will be null
    val currentSum: (Long, Double) = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0d)
    }

    // update the count
    val newSum: (Long, Double) = (currentSum._1 + 1, currentSum._2 + in._2)

    // update the state
    sum.update(newSum)

    // if the count reaches 2, emit the average and clear the state
    if (newSum._1 >= 2) {
      out.collect((in._1, newSum._2/newSum._1))
    }
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Double)]("average", classOf[(Long, Double)])
    )
  }
}
