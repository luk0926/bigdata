package day04.cep

import java.util
import java.util.Collections

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day04.cep
 * @Author: luk
 * @CreateTime: 2020/3/25 17:22
 */

case class UserLogin(ip: String, username: String, operateUrl: String, time: String)

object CheckIPChange {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val socketStream: DataStream[String] = env.socketTextStream("node03", 9999)

    socketStream.map(t => {
      val strings: Array[String] = t.split(",")

      (strings(1), UserLogin(strings(0), strings(1), strings(2), strings(3)))
    }).keyBy(_._1)
      .process(new LoginCheckProcessFunction)
        .print()


    env.execute()
  }
}

class LoginCheckProcessFunction extends KeyedProcessFunction[String, (String, UserLogin), (String, UserLogin)] {

  private var listState: ListState[UserLogin] = _

  override def open(parameters: Configuration): Unit = {
    val listStateDescriptor: ListStateDescriptor[UserLogin] = new ListStateDescriptor[UserLogin]("changeIp", classOf[UserLogin])

    listState = getRuntimeContext.getListState(listStateDescriptor)
  }

  override def processElement(value: (String, UserLogin), ctx: KeyedProcessFunction[String, (String, UserLogin), (String, UserLogin)]#Context, out: Collector[(String, UserLogin)]): Unit = {
    val logins: util.ArrayList[UserLogin] = new util.ArrayList[UserLogin]()

    listState.add(value._2)

    import scala.collection.JavaConverters._

    val list: List[UserLogin] = listState.get().asScala.toList

    list.sortBy(_.time)

    if (list.size == 2) {
      val first: UserLogin = list(0)
      val second: UserLogin = list(1)

      if(!first.ip.equals(second.ip)) {
        println("ip已改变，请重新登录")
      }

      //移除第一个ip，保留第二个ip即可
      logins.removeAll(Collections.EMPTY_LIST)
      logins.add(second)
      listState.update(logins)
    }

    out.collect(value)
  }
}