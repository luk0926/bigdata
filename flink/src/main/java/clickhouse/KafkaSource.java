package clickhouse;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: clickhouse
 * @Author: luk
 * @CreateTime: 2020/3/27 14:15
 */
public class KafkaSource {
    private static String topic = "test";

    public static FlinkKafkaConsumer<String> getKafkaSource() {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        prop.setProperty("group.id","con1");
        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        return flinkKafkaConsumer;
    }
}
