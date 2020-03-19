package day02.hbase

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.hbase
 * @Author: luk
 * @CreateTime: 2020/3/19 18:14
 */
object FlinkReadHBase {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val hbaseData: DataSet[tuple.Tuple2[String, String]] = environment.createInput(new TableInputFormat[tuple.Tuple2[String, String]] {
      override def configure(parameters: Configuration): Unit = {
        val conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, "node01,node02,node03")
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
        val conn: Connection = ConnectionFactory.createConnection(conf)
        table = classOf[HTable].cast(conn.getTable(TableName.valueOf("hbasesource")))
        scan = new Scan() {
          // setStartRow(Bytes.toBytes("1001"))
          // setStopRow(Bytes.toBytes("1004"))
          addFamily(Bytes.toBytes("f1"))
        }
      }
      override def getScanner: Scan = {
        scan
      }
      override def getTableName: String = {
        "hbasesource"
      }
      override def mapResultToTuple(result: Result): tuple.Tuple2[String, String] = {
        val rowkey: String = Bytes.toString(result.getRow)
        val sb = new StringBuffer()
        for (cell: Cell <- result.rawCells()) {
          val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          sb.append(value).append(",")
        }
        val valueString = sb.replace(sb.length() - 1, sb.length(), "").toString
        val tuple2 = new org.apache.flink.api.java.tuple.Tuple2[String, String]
        tuple2.setField(rowkey, 0)
        tuple2.setField(valueString, 1)
        tuple2
      }


    })
    hbaseData.print()
    environment.execute()
  }
}
