package window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: window
 * @Author: luk
 * @CreateTime: 2020/4/2 14:02
 * <p>
 * 通过接收socket当中输入的数据，统计每5秒钟数据的累计的值
 */
public class TimeWindowsCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("node03", 9999);

        dataStreamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        }).timeWindowAll(Time.seconds(5))
                .process(new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                        int sum = 0;
                        Iterator<Integer> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            sum += iterator.next();
                        }
                        out.collect(sum);
                    }
                }).print();


        env.execute("TimeWindowsCount");
    }
}
