package day01.partitionjava;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.swing.*;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.partitionjava
 * @Author: luk
 * @CreateTime: 2020/3/18 14:41
 */
public class CustomerPartitionerJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> dataStreamSource = env.fromElements("hello spark", "hello hadoop", "hadoop flink");

        SingleOutputStreamOperator<Tuple1<String>> tupleDataStream = dataStreamSource.map(new MapFunction<String, Tuple1<String>>() {
            @Override
            public Tuple1<String> map(String s) throws Exception {
                return new Tuple1<>(s);
            }
        });

        DataStream<Tuple1<String>> partitionStream = tupleDataStream.partitionCustom(new MyPartitionerJava(), 0);

        partitionStream.map(new MapFunction<Tuple1<String>, String>() {
            @Override
            public String map(Tuple1<String> stringTuple1) throws Exception {
                System.out.println("当前线程："+Thread.currentThread().getId() + ", value:" + stringTuple1.f0);

                return stringTuple1.f0;
            }
        });

        env.execute();
    }
}
