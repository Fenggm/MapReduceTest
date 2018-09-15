package com.fgm.demo2.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 9:56
 */
public class SortReducer extends Reducer<SortPojo,IntWritable,Text,IntWritable> {
    private Text outputKey=new Text();

    @Override
    protected void reduce(SortPojo key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            outputKey.set(key.getFirst());
            context.write(outputKey,value);
        }
    }
}
