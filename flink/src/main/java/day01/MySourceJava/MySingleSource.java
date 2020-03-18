package day01.MySourceJava;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.MySourceJava
 * @Author: luk
 * @CreateTime: 2020/3/18 10:27
 */
public class MySingleSource implements SourceFunction<Long> {
    private Long number = 0L;
    private boolean isRunning = true;

    /**
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            number += 1;
            ctx.collect(number);
            Thread.sleep(1000);
        }
    }

    /**
     *
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
