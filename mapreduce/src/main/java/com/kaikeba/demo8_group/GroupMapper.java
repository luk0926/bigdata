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
 * @CreateTime: 2019/12/8 10:51
 */
public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean;

    /**
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.orderBean = new OrderBean();
    }

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        String orderId = split[0];
        double price = Double.valueOf(split[2]);

        orderBean.setOrderId(orderId);
        orderBean.setPrice(price);

        context.write(orderBean, NullWritable.get());
    }
}
