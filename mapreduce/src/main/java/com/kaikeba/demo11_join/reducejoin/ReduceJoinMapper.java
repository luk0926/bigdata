package com.kaikeba.demo11_join.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo11_join
 * @Author: luk
 * @CreateTime: 2019/12/10 14:27
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * //现在我们读取了两个文件，如何确定当前处理的这一行数据是来自哪一个文件里面的
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*   //通过文件名判断.获取文件的切片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();//获取我们输入的文件的切片
//获取文件名称
        String name = inputSplit.getPath().getName();
        if(name.equals("orders.txt")){
            //订单表数据
        }else{
            //商品表数据
        }*/
        String[] split = value.toString().split(",");
        if (value.toString().startsWith("p")) {
            //以商品id作为key2,相同商品的数据都会到一起去
            context.write(new Text(split[0]), value);
        } else {
            context.write(new Text(split[2]), value);
        }
    }
}
