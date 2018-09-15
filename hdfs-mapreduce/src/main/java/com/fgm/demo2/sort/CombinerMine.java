package com.fgm.demo2.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 18:06
 */
public class CombinerMine extends Reducer<SortPojo,IntWritable,SortPojo,IntWritable> {

    /**
     * 设置我们的combine,在我们的map端对数据做一次局部的聚合.
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(SortPojo key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for (IntWritable value : values) {
            context.write(key,value);
        }

    }
}
