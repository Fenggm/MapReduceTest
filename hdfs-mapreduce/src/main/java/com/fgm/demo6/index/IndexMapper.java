package com.fgm.demo6.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/2 23:16
 */
public class IndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text text=new Text();
    IntWritable times=new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();

        String[] split = value.toString().split(" ");
        for (String word : split) {
            text.set(word+"-"+name);
            times.set(1);
            context.write(text,times);
        }

    }
}
