package tableapi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day04.tableapi
 * @Author: luk
 * @CreateTime: 2020/3/25 14:33
 */
public class DataStream2Table {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> dataStream = env.socketTextStream("node03", 9999);

        SingleOutputStreamOperator<User> map = dataStream.map(new MapFunction<String, User>() {
            @Override
            public User map(String s) throws Exception {
                String[] split = s.split(",");
                User user = new User();
                user.setId(Integer.parseInt(split[0]));
                user.setName(split[1]);
                user.setAge(Integer.parseInt(split[2]));
                return user;
            }
        });

        tEnv.registerDataStream("myUser", map);

        Table table = tEnv.sqlQuery("select * from myUser where age > 20");

        //tEnv.toRetractStream(table, Row.class).print();

        /**
         * 使用append模式将Table转换成为dataStream，
         * 不能用于sum，count，avg等操作，
         * 只能用于添加数据操作
         */
        //DataStream<Row> rowDataStream = tEnv.toAppendStream(table, Row.class);
        //rowDataStream.print();

        /**
         *  使用retract模式将Table转换成为DataStream
         */
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(table, Row.class);
        tuple2DataStream.print();

        env.execute();
    }

    public static class User{
        private Integer id;
        private String name;
        private Integer age;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }
}
