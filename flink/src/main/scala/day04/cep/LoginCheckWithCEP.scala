package day04.cep

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day04.cep
 * @Author: luk
 * @CreateTime: 2020/3/25 17:42
 */
object LoginCheckWithCEP {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val dataStream: DataStream[String] = env.socketTextStream("node03", 9999)

    val result: KeyedStream[(String, UserLogin), String] = dataStream.map(x => {
      val strings: Array[String] = x.split(",")
      (strings(1), UserLogin(strings(0), strings(1), strings(2), strings(3)))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, UserLogin)](Time.seconds(5)) {
      override def extractTimestamp(element: (String, UserLogin)): Long = {
        element._2.time
        val time: Long = format.parse(element._2.time).getTime
        time
      }
    }).keyBy(_._1)

    val pattern: Pattern[(String, UserLogin), (String, UserLogin)] = Pattern
      .begin[(String, UserLogin)]("begin")
      .where(x => {
        x._2.username != null
      }).next("second")
      .where(new IterativeCondition[(String, UserLogin)] {
        override def filter(value: (String, UserLogin), ctx: IterativeCondition.Context[(String, UserLogin)]): Boolean = {
          var flag: Boolean = false
          val firstValues: util.Iterator[(String, UserLogin)] = ctx.getEventsForPattern("begin").iterator()

          while (firstValues.hasNext) {
            val tuple: (String, UserLogin) = firstValues.next()

            if (!tuple._2.ip.equals(value._2.ip)) {
              flag = true
            }
          }
          flag
        }
      })
      .within(Time.seconds(120))

    //应用模式
    val patternStream: PatternStream[(String, UserLogin)] = CEP.pattern(result, pattern)
    patternStream.select(new CEPatternFunction).print()

    env.execute()
  }
}

class CEPatternFunction extends PatternSelectFunction[(String,UserLogin),(String,UserLogin)]{
  override def select(map: util.Map[String, util.List[(String, UserLogin)]]): (String, UserLogin) = {
    val iter = map.get("begin").iterator()
    val tuple: (String, UserLogin) = map.get("second").iterator().next()
    val scalaIterable: Iterable[util.List[(String, UserLogin)]] = map.values().asScala
    for(eachIterable <- scalaIterable){
      if(eachIterable.size() > 0){
        val scalaListBuffer: mutable.Buffer[(String, UserLogin)] = eachIterable.asScala
        for(eachTuple <- scalaListBuffer){
          // println(eachTuple._2.operateUrl)
        }
      }
    }
    tuple
  }
}