package com.kaikeba.demo5_partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo5
 * @Author: luk
 * @CreateTime: 2019/12/5 22:07
 */
public class PartitionOwn extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phoneNum = text.toString();
        if (phoneNum != null && !phoneNum.equals("")) {
            if (phoneNum.startsWith("135")) {
                return 0;
            } else if (phoneNum.startsWith("136")) {
                return 1;
            } else if (phoneNum.startsWith("137")) {
                return 2;
            } else if (phoneNum.startsWith("138")) {
                return 3;
            } else if (phoneNum.startsWith("139")) {
                return 4;
            } else {
                return 5;
            }
        } else {
            return 5;
        }

    }
}
