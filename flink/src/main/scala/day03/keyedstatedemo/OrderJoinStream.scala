package day03.keyedstatedemo

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.io.{BufferedSource, Source}
import scala.util.Random

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day03.keyedstatedemo
 * @Author: luk
 * @CreateTime: 2020/3/23 14:57
 */
object OrderJoinStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //读取自定义两个数据源
    val orderInfo1: DataStream[String] = env.addSource(new FileSourceFunction("D:\\资料\\开课吧\\20_flink进阶\\3、flink-第三次课——课前资料\\数据\\orderInfo1.txt"))
    val orderInfo2: DataStream[String] = env.addSource(new FileSourceFunction("D:\\资料\\开课吧\\20_flink进阶\\3、flink-第三次课——课前资料\\数据\\orderInfo2.txt"))

    //订单一数据按照订单id进行分组
    val orderInfo1Stream: KeyedStream[OrderInfo1, Long] = orderInfo1.map(x => OrderInfo1(x.split(",")(0).toLong, x.split(",")(1), x.split(",")(2).toDouble))
      .keyBy(x => x.orderId)

    //订单二数据按照订单id进行分组
    val orderInfo2Stream: KeyedStream[OrderInfo2, Long] = orderInfo2.map(x => OrderInfo2(x.split(",")(0).toLong, x.split(",")(1), x.split(",")(2)))
      .keyBy(x => x.orderId)

    orderInfo1Stream.connect(orderInfo2Stream)
        .flatMap(new MyRichFlatMapFunction)
        .print()

    env.execute()
  }
}

class MyRichFlatMapFunction extends RichCoFlatMapFunction[OrderInfo1, OrderInfo2, (OrderInfo1,OrderInfo2)] {
  private var  orderInfo1ValueState:ValueState[OrderInfo1] = _
  private var  orderInfo2ValueState:ValueState[OrderInfo2] = _

  //获取每个订单对应的存储的状态
  override def open(parameters: Configuration): Unit = {
    orderInfo1ValueState = getRuntimeContext.getState(new ValueStateDescriptor[OrderInfo1]("order1InfoState", classOf[OrderInfo1]))
    orderInfo2ValueState = getRuntimeContext.getState(new ValueStateDescriptor[OrderInfo2]("order2InfoState",classOf[OrderInfo2]))
  }

  /**
   * 针对订单一使用的方法
   * @param orderInfo1
   * @param out
   */
  override def flatMap1(orderInfo1: OrderInfo1, out: Collector[(OrderInfo1, OrderInfo2)]): Unit = {
    val orderInfo2: OrderInfo2 = orderInfo2ValueState.value()

    if (orderInfo2 != null) {
      orderInfo2ValueState.clear
      out.collect(orderInfo1,orderInfo2)
    } else {
      orderInfo1ValueState.update(orderInfo1)
    }
  }

  /**
   * 针对订单二使用的方法
   * @param orderInfo2
   * @param out
   */
  override def flatMap2(orderInfo2: OrderInfo2, out: Collector[(OrderInfo1, OrderInfo2)]): Unit = {
    val orderInfo1: OrderInfo1 = orderInfo1ValueState.value()
    if(orderInfo1 != null) {
      orderInfo1ValueState.clear()
      out.collect((orderInfo1, orderInfo2))
    } else {
      orderInfo2ValueState.update(orderInfo2)
    }
  }
}

class FileSourceFunction(filePath:String) extends SourceFunction[String] {
  private val random: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val bufferedSource: BufferedSource = Source.fromFile(filePath,"GBK")
    val lines: Iterator[String] = bufferedSource.getLines()
    while (lines.hasNext){
      TimeUnit.MILLISECONDS.sleep(random.nextInt(500))
      ctx.collect(lines.next())
    }
    bufferedSource.close()
  }

  override def cancel(): Unit = {

  }
}