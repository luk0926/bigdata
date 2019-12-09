package com.kaikeba.demo9_topN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo8_group
 * @Author: luk
 * @CreateTime: 2019/12/9 14:27
 */
public class GroupReduce extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (NullWritable value : values) {
            if (i < 2) {
                context.write(key, NullWritable.get());
                i += 1;
            } else {
                break;
            }
        }


    }
}
