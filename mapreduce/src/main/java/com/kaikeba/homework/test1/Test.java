package com.kaikeba.homework.test1;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.homework.test1
 * @Author: luk
 * @CreateTime: 2019/12/9 17:47
 */
public class Test {
    public static void main(String[] args) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String format = sdf.format(date);
        System.out.println(format);

        String s = "";
        System.out.println(s.isEmpty());

        boolean b = true;
        System.out.println(b==true);
    }
}
