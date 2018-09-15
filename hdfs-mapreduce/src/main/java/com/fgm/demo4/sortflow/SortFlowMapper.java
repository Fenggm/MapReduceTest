package com.fgm.demo4.sortflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 19:24
 */
public class SortFlowMapper extends Mapper<LongWritable, Text, SortFlowBean, Text> {

    Text phoneNum=new Text();
    SortFlowBean flowBean=new SortFlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");

        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));

        phoneNum.set(split[0]);
        context.write(flowBean,phoneNum);
    }
}
