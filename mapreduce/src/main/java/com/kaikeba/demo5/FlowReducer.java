package com.kaikeba.demo5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer  extends Reducer<Text,FlowBean,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int donwFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            donwFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        context.write(key,new Text(upFlow +"\t" +  donwFlow + "\t" +  upCountFlow + "\t" + downCountFlow));
    }
}
