package day02.hbase

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day02.hbase
 * @Author: luk
 * @CreateTime: 2020/3/20 10:52
 */
class HBaseSink extends RichSinkFunction[String]{
  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "node01")
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    conn = ConnectionFactory.createConnection(conf)
  }

  override def invoke(value: String): Unit = {
    val t: Table = conn.getTable(TableName.valueOf("hbasesource"))
    val cf1 = "f1"
    val array: Array[String] = value.split(",")
    val put: Put = new Put(Bytes.toBytes(array(0)))

    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array(1)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array(2)))
    val putList: java.util.ArrayList[Put] = new java.util.ArrayList[Put]
    putList.add(put)

    t.put(put)
    t.close()
  }

  override def close(): Unit = {
    super.close()
    conn.close()
  }

}
