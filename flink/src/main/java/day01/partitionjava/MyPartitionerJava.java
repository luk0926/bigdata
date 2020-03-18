package day01.partitionjava;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01.partitionjava
 * @Author: luk
 * @CreateTime: 2020/3/18 14:32
 */
public class MyPartitionerJava implements Partitioner<String> {

    @Override
    public int partition(String s, int i) {
        if (s.contains("hello")){
            return 1;
        }else {
            return 0;
        }
    }
}
