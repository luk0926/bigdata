package com.kaikeba.hbase.demo1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.hbase.demo1
 * @Author: luk
 * @CreateTime: 2019/12/26 11:01
 */
public class HBaseWriteReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {

    //将map传输过来的数据，写入到hbase表

    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
        immutableBytesWritable.set(key.toString().getBytes());

        //遍历put，并输出
        for (Put put : values) {
            context.write(immutableBytesWritable, put);
        }
    }
}
