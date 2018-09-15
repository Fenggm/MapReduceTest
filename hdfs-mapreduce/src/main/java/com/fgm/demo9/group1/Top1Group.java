package com.fgm.demo9.group1;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 *
 * @Auther: fgm
 * @Date: 2018/9/3 21:25
 */
public class Top1Group extends WritableComparator {

    public Top1Group(){
        //通过构造器,来指定我们的反射类,反射出来的是哪个javabean
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first= (OrderBean) a;

        OrderBean second= (OrderBean) b;

        return first.getOrderId().compareTo(second.getOrderId());

    }
}
