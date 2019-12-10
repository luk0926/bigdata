package com.kaikeba.homework.test;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.homework.test
 * @Author: luk
 * @CreateTime: 2019/12/10 11:38
 */
public class LogReducer extends Reducer<LogBean, NullWritable, LogBean, NullWritable>{

    @Override
    protected void reduce(LogBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }

}
