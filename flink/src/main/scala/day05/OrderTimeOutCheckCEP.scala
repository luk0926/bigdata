package day05

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day05
 * @Author: luk
 * @CreateTime: 2020/3/26 16:42
 */
case class OrderDetail(orderId:String,status:String,orderCreateTime:String,price :Double)

object OrderTimeOutCheckCEP {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val dataStream: DataStream[String] = env.socketTextStream("node03", 9999)

    val keyedStream: KeyedStream[OrderDetail, String] = dataStream.map(x => {
      val strings: Array[String] = x.split(",")
      OrderDetail(strings(0), strings(1), strings(2), strings(3).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderDetail](Time.seconds(5)) {
      override def extractTimestamp(element: OrderDetail): Long = {
        format.parse(element.orderCreateTime).getTime
      }
    }).keyBy(_.orderId)

    val pattern: Pattern[OrderDetail, OrderDetail] = Pattern.begin[OrderDetail]("begin")
      .where(_.status.equals("1"))
      .followedBy("second")
      .where(_.status.equals("2"))
      .within(Time.minutes(15))

    //调用select方法，提取事件序列，超时的事件要做报警提示
    val orderTimeoutOutputTag: OutputTag[OrderDetail] = new OutputTag[OrderDetail]("orderTimeout")

    val patternStream: PatternStream[OrderDetail] = CEP.pattern(keyedStream,pattern)

    val selectResultStream: DataStream[OrderDetail] = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutPatternFunction, new OrderPatternFunction)

    selectResultStream.print()

    //打印侧输出流数据 过了15分钟还没支付的数据
    selectResultStream.getSideOutput(orderTimeoutOutputTag).print()

    env.execute()
  }
}

class OrderTimeoutPatternFunction extends PatternTimeoutFunction[OrderDetail,OrderDetail]{
  override def timeout(pattern: util.Map[String, util.List[OrderDetail]], timeoutTimestamp: Long): OrderDetail = {
    val detail: OrderDetail = pattern.get("start").iterator().next()
    println("超时订单号为" + detail)
    detail
  }
}

class OrderPatternFunction extends PatternSelectFunction[OrderDetail,OrderDetail]{
  override def select(pattern: util.Map[String, util.List[OrderDetail]]): OrderDetail = {
    val detail: OrderDetail = pattern.get("second").iterator().next()
    println("支付成功的订单为" + detail)
    detail
  }

}