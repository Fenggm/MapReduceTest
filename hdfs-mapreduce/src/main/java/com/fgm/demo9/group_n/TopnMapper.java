package com.fgm.demo9.group_n;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/3 21:07
 */
public class TopnMapper extends Mapper<LongWritable,Text,OrderBean,Text> {

    OrderBean orderBean=new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        orderBean.setOrderId(split[0]);
        orderBean.setPrice(Double.valueOf(split[2]));
        context.write(orderBean,value);
    }
}
