package com.kaikeba.demo5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionOwn extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phoenNum = text.toString();
        if(null != phoenNum && !phoenNum.equals("")){
            if(phoenNum.startsWith("135")){
                return 0;
            }else if(phoenNum.startsWith("136")){
                return 1;
            }else if(phoenNum.startsWith("137")){
                return 2;
            }else if(phoenNum.startsWith("138")){
                return 3;
            }else if(phoenNum.startsWith("139")){
                return 4;
            }else {
                return 5;
            }
        }else{
            return 5;
        }
    }
}