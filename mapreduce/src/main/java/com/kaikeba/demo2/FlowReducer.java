package com.kaikeba.demo2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo2
 * @Author: luk
 * @CreateTime: 2019/12/4 17:18
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {

    /**
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int downFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;

        for (FlowBean flowBean : values) {
            upFlow += flowBean.getUpFlow();
            downFlow += flowBean.getDownFlow();
            upCountFlow += flowBean.getUpCountFlow();
            downCountFlow += flowBean.getDownCountFlow();
        }

        context.write(key, new Text(upFlow + "\t" + downFlow + "\t" + upCountFlow + "\t" + downCountFlow));
    }
}
