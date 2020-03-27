package clickhouse;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

import java.sql.*;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: clickhouse
 * @Author: luk
 * @CreateTime: 2020/3/27 14:16
 */
public class DataClean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //checkpoint**配置
        /*env.enableCheckpointing(100);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        env.setStateBackend(new RocksDBStateBackend("hdfs://node01:8020/flink_kafka_sink/checkpoints", true));
*/
        //kafka中获取数据
        //DataStreamSource<String> dataStreamSource = env.addSource(KafkaSource.getKafkaSource());
        DataStreamSource<String> dataStreamSource = env.socketTextStream("node03", 9999);

        Connection clickHouseConnection = ClickhouseUtil.getClickHouseConnection();

        SingleOutputStreamOperator<UserBean> userBeanStream = dataStreamSource.map(new MapFunction<String, UserBean>() {
            @Override
            public UserBean map(String value) throws Exception {
                String[] split = value.split(",");
                UserBean userBean = new UserBean();

                userBean.setId(Integer.parseInt(split[0]));
                userBean.setName(split[1]);
                userBean.setAge(Integer.parseInt(split[2]));
                userBean.setData_date(split[3]);

                PreparedStatement statement = clickHouseConnection.prepareStatement("insert into ods_user_login(id, name, age, data_date) values(?, ?, ?, ?)");
                statement.setInt(1, Integer.parseInt(split[0]));
                statement.setString(2, split[1]);
                statement.setInt(3, Integer.parseInt(split[2]));
                statement.setString(4, split[3]);

                statement.execute();
                return userBean;
            }
        });


        /*tEnv.registerDataStream("myUser", userBeanStream);
        Table result = tEnv.sqlQuery("select * from myUser");

        String targetTable = "ods_user_login";
        TypeInformation[] fieldTypes = {BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
        TableSink jdbcSink = JDBCAppendTableSink.builder()
                .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
                .setDBUrl("jdbc:clickhouse://node03:8123")
                .setQuery("insert into ods_user_login(id, name, age, data_date) values(?, ?, ?, ?)")
                .setParameterTypes(fieldTypes)
                .setBatchSize(2)
                .build();

        tEnv.registerTableSink(targetTable, new String[]{"id", "name", "age", "data_date"}, new TypeInformation[]{Types.INT(), Types.STRING(), Types.INT(), Types.STRING()}, jdbcSink);

        result.insertInto(targetTable);*/

        env.execute();
    }
}

class ClickhouseUtil {
    static String address = "jdbc:clickhouse://node03:8123/default";
    static Connection connection = null;

    public static Connection getClickHouseConnection() throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        connection = DriverManager.getConnection(address);

        return connection;
    }
}