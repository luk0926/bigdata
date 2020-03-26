package day05

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day05
 * @Author: luk
 * @CreateTime: 2020/3/26 15:19
 */

//定义温度信息pojo
case class DeviceDetail(sensorMac:String,deviceMac:String,temperature:String,dampness:String,pressure:String,date:String)
case class AlarmDevice(sensorMac:String,deviceMac:String,temperature:String)

object FlinkTempeatureCEP {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-HH-mm HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val dataStream: DataStream[String] = env.socketTextStream("node03", 9999)

    val deviceStream: KeyedStream[DeviceDetail, String] = dataStream.map(x => {
      val strings: Array[String] = x.split(",")
      DeviceDetail(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
    }).assignAscendingTimestamps(x => {
      format.parse(x.date).getTime
    }).keyBy(_.sensorMac)

    val pattern: Pattern[DeviceDetail, DeviceDetail] = Pattern
      .begin[DeviceDetail]("start")
      .where(_.temperature.toInt >= 40)
      .followedByAny("follow")
      .where(_.temperature.toInt >= 40)
      .followedByAny("third")
      .where(_.temperature.toInt >= 40)
      .within(Time.minutes(3))

    val patternResult: PatternStream[DeviceDetail] = CEP.pattern(deviceStream, pattern)

    patternResult.select(new MyPatternResultFunction).print()

    env.execute()
  }
}

class MyPatternResultFunction extends PatternSelectFunction[DeviceDetail, AlarmDevice] {
  override def select(pattern: util.Map[String, util.List[DeviceDetail]]): AlarmDevice = {

    val startDetails: util.List[DeviceDetail] = pattern.get("start")
    val followDetails: util.List[DeviceDetail] = pattern.get("follow")
    val thirdDetails: util.List[DeviceDetail] = pattern.get("third")

    val startResult: DeviceDetail = startDetails.listIterator().next()
    val followResult: DeviceDetail = followDetails.iterator().next()
    val thirdResult: DeviceDetail = thirdDetails.iterator().next()

    println("第一条数据"+startResult)
    println("第二条数据"+followResult)
    println("第三条数据"+thirdResult)

    AlarmDevice(thirdResult.sensorMac,thirdResult.deviceMac,thirdResult.temperature)
  }
}