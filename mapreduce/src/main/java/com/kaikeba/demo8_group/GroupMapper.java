package com.kaikeba.demo8_group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo8_group
 * @Author: luk
 * @CreateTime: 2019/12/9 14:21
 */
public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.orderBean = new OrderBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        orderBean.setOrderId(split[0]);
        orderBean.setPrice(Double.parseDouble(split[2]));

        context.write(orderBean, NullWritable.get());
    }
}
