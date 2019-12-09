package com.kaikeba.demo9_topN;

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
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
