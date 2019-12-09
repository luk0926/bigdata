package com.kaikeba.demo8_group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba.demo8_group
 * @Author: luk
 * @CreateTime: 2019/12/8 11:08
 */
public class MyGroup extends WritableComparator {

    /**
     * 覆写默认构造器，通过反射，构造OrderBean对象
     * 通过反射来构造OrderBean对象
     * 接受到的key2  是orderBean类型，我们就需要告诉分组，以orderBean接受我们的参数
     */
    public MyGroup() {
        super(OrderBean.class, true);
    }

    /**
     * compare方法接受到两个参数，这两个参数其实就是我们前面传过来的OrderBean
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;

        //以orderId作为比较条件，判断哪些orderid相同作为同一组
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
