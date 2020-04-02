package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: window
 * @Author: luk
 * @CreateTime: 2020/4/2 14:24
 */
public class CountWindos {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("node03", 9999);

        dataStreamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        }).countWindowAll(5)
                /*.reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer integer, Integer t1) throws Exception {
                        return t1 + integer;
                    }
                }).print();*/
                .apply(new AllWindowFunction<Integer, Integer, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception {
                        int sum = 0;
                        Iterator<Integer> iterator = values.iterator();
                        while (iterator.hasNext()){
                            sum += iterator.next();
                        }
                        out.collect(sum);
                    }
                }).print();


        env.execute("CountWindos");
    }
}
