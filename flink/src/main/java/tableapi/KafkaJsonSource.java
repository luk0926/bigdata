package tableapi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: tableapi
 * @Author: luk
 * @CreateTime: 2020/4/7 16:00
 */
public class KafkaJsonSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Kafka kafka = new Kafka()
                .version("0.11")
                .topic("kafka_source_table")
                .startFromEarliest()
                .property("group.id", "test_group")
                .property("bootstrap.servers", "node01:9092,node02:9092,node03:9092");

        Json json = new Json().failOnMissingField(false).deriveSchema();

        Schema schema = new Schema()
                .field("userId", Types.INT)
                .field("day", Types.STRING)
                .field("begintime", Types.LONG)
                .field("endtime", Types.LONG);

        tableEnv.connect(kafka)
                .withFormat(json)
                .withSchema(schema)
                .inAppendMode()
                .registerTableSource("user_log");

        Table table = tableEnv.sqlQuery("select userId,`day` ,begintime,endtime  from user_log");

        table.printSchema();

        tableEnv.toRetractStream(table, Row.class).print();

        env.execute("KafkaJsonSource");
    }
}
