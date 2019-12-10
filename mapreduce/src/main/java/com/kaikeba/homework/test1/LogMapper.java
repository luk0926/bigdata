package com.kaikeba.homework.test1;

import org.apache.avro.JsonProperties;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.homework.test1
 * @Author: luk
 * @CreateTime: 2019/12/10 11:07
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text text;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.text = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("MR_COUNT", "MapRecordCounter");

        String[] split = value.toString().split("\t");
        int length = split.length;

        if (length == 6) {
            String dateTime = split[0];
            String userId = split[1];
            String searchkwd = split[2];
            String retorder = split[3];
            String cliorder = split[4];
            String cliurl = split[5];

            //判断是否是正确的url
            boolean b = isHttpUrl(cliurl);

            try {
                if (dateTime.length() == 14 && !userId.isEmpty() && !searchkwd.isEmpty() && !retorder.isEmpty() && !cliorder.isEmpty() && !cliurl.isEmpty() && b == true) {

                    context.write(text, NullWritable.get());
                } else {
                    counter.increment(1L);
                }
            } catch (Exception e) {
                counter.increment(1L);
            }
        }else {
            counter.increment(1L);
        }

    }

    public static boolean isHttpUrl (String urls){
        boolean isurl = false;
        String regex = "(((https|http)?://)?([a-z0-9]+[.])|(www.))"
                + "\\w+[.|\\/]([a-z0-9]{0,})?[[.]([a-z0-9]{0,})]+((/[\\S&&[^,;\u4E00-\u9FA5]]+)+)?([.][a-z0-9]{0,}+|/?)";//设置正则表达式

        Pattern pat = Pattern.compile(regex.trim());//对比
        Matcher mat = pat.matcher(urls.trim());
        isurl = mat.matches();//判断是否匹配
        if (isurl) {
            isurl = true;
        }
        return isurl;
    }
}
