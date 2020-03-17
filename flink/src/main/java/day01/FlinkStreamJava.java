package day01;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01
 * @Author: luk
 * @CreateTime: 2020/3/17 16:14
 */
public class FlinkStreamJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> socket = env.socketTextStream("node03", 9999);

        SingleOutputStreamOperator<WordCountJava> wordCountStream = socket.flatMap(new FlatMapFunction<String, WordCountJava>() {
            @Override
            public void flatMap(String s, Collector<WordCountJava> out) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    out.collect(new WordCountJava(word, 1));
                }
            }
        });

        SingleOutputStreamOperator<WordCountJava> resultStream = wordCountStream.keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1)).sum("count");
        resultStream.print();

        env.execute();
    }
}
