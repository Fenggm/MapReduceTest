package com.fgm.demo2.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 9:56
 */
public class SortMapper extends Mapper<LongWritable,Text,SortPojo,IntWritable> {

    private SortPojo mapOutKey =new SortPojo();
    private IntWritable mapOutValue=new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //自定义我们的计数器,这里实现了统计map数据的条数
        Counter counter = context.getCounter("MR_COUNT", "MapRecordCounter");
        counter.increment(1L);

        String lineValue = value.toString();
        String[] split = lineValue.split("\t");

        mapOutKey.set(split[0],Integer.valueOf(split[1]));
        mapOutValue.set(Integer.valueOf(split[1]));

        context.write(mapOutKey,mapOutValue);

    }
}
