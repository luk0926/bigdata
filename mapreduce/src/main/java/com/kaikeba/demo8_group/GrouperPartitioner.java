package com.kaikeba.demo8_group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo8_group
 * @Author: luk
 * @CreateTime: 2019/12/9 14:30
 */
public class GrouperPartitioner extends Partitioner<OrderBean, NullWritable> {


    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
        //注意这里：使用orderId作为分区的条件，来进行判断，保证相同的orderId进入到同一个reduceTask里面去
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}