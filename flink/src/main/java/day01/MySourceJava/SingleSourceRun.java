package day01.MySourceJava;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.MySourceJava
 * @Author: luk
 * @CreateTime: 2020/3/18 10:35
 */
public class SingleSourceRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source = env.addSource(new MySingleSource());
        SingleOutputStreamOperator<Long> filterStream = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        filterStream.print();

        env.execute();
    }
}
