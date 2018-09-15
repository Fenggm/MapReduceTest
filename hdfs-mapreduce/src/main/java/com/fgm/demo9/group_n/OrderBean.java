package com.fgm.demo9.group_n;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/3 21:08
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + '\t' + price;
    }

    @Override
    public int compareTo(OrderBean o) {
        //判断订单id是否相同,相同则比较,不同不比较;
        int i = this.orderId.compareTo(o.orderId);
        if (i==0){
            i=this.price.compareTo(o.price);
        }
        //从大到小进行排序
        return -i;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId=in.readUTF();
        this.price=in.readDouble();
    }
}
