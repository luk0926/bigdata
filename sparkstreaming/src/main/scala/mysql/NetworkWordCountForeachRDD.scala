package mysql


import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: wordcount
 * @Author: luk
 * @CreateTime: 2020/3/12 17:50
 */
object NetworkWordCountForeachRDD {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("node03", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val wordCounts: DStream[(String, Int)] = socket.flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    //将结果写入到mysql
    /*wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreach { record =>
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        statement.setLong(1, time.milliseconds)
        statement.setString(2, record._1)
        statement.setInt(3, record._2)
        statement.execute()
        statement.close()
        conn.close()
      }
    }*/

    wordCounts.foreachRDD((rdd, time) => {
      rdd.foreachPartition(partitionsRecords => {
        val conn: Connection = ConnectionPool.getConnection

        conn.setAutoCommit(false)

        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")

        partitionsRecords.foreach {
          case (word, count) => {
            statement.setLong(1, time.milliseconds)
            statement.setString(2, word)
            statement.setLong(3, count)
            statement.addBatch()
          }

            statement.executeBatch()
            statement.close()
            conn.commit()
            conn.setAutoCommit(true)
            ConnectionPool.returnConnection(conn)
        }
      })
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
