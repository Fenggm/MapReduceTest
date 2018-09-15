package com.fgm.demo9.group_n;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/3 21:07
 */
public class TopnPartition extends Partitioner<OrderBean,Text> {

    @Override
    public int getPartition(OrderBean orderBean, Text text, int numPartitions) {
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
