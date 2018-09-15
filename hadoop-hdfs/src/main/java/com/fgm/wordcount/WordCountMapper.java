package com.fgm.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 0:14
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    /**
     *
     * @param key  k1
     * @param value   v1
     * @param context 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(",");
        for (String word : split) {
            //获取单词,写出
            context.write(new Text(word),new IntWritable(1));
        }

    }
}
