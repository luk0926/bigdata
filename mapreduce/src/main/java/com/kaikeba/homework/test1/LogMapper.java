package com.kaikeba.homework.test1;


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
 * @BelongsPackage: com.kaikeba.test
 * @Author: luk
 * @CreateTime: 2019/12/9 17:33
 */
public class LogMapper extends Mapper<LongWritable, Text, LogBean, NullWritable> {

    private LogBean logBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.logBean = new LogBean();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Counter counter = context.getCounter("COUNT", "MapRecordCounter");

        String[] split = value.toString().split("\t");
        int length = split.length;

        String dateTime = split[0];
        String userId = split[1];
        String searchkwd = split[2];
        String retorder = split[3];
        String cliorder = split[4];
        String cliurl = split[5];

        try{
            logBean.setDateTime(Long.parseLong(dateTime));
            logBean.setUserId(userId);
            logBean.setSearchkwd(searchkwd);
            logBean.setRetorder(Integer.parseInt(retorder));
            logBean.setCliorder(Integer.parseInt(cliorder));
            logBean.setCliurl(cliurl);

            context.write(logBean, NullWritable.get());
        }catch (Exception e){
            e.printStackTrace();
        }



        /*if (length == 6) {
            String dateTime = split[0];
            String userId = split[1];
            String searchkwd = split[2];
            String retorder = split[3];
            String cliorder = split[4];
            String cliurl = split[5];

            //判断是否是正确的url
            boolean b = isHttpUrl(cliurl);

            try {
                if (dateTime.length() == 14 && !userId.isEmpty() && !searchkwd.isEmpty() && !retorder.isEmpty() && !cliorder.isEmpty() && !cliurl.isEmpty() && b==true) {
                    logBean.setDateTime(Long.parseLong(dateTime));
                    logBean.setUserId(userId);
                    logBean.setSearchkwd(searchkwd);
                    logBean.setRetorder(Integer.parseInt(retorder));
                    logBean.setCliorder(Integer.parseInt(cliorder));
                    logBean.setCliurl(cliurl);

                    context.write(logBean, NullWritable.get());
                } else {
                    counter.increment(1L);
                }
            } catch (Exception e) {
                counter.increment(1L);
            }
        }*/
    }

    public static boolean isHttpUrl(String urls) {
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
