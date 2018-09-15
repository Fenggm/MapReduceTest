package com.fgm.demo6.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/2 23:16
 */
public class IndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable times=new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable value : values) {
            sum+=Integer.parseInt(value.toString());
        }
        times.set(sum);
        context.write(key,times);
    }
}
