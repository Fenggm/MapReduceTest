package com.fgm.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 0:26
 * 继承reducer类,来实现我们的数据的进一步的处理,需要四个泛型
 * 输入的是k2,v2
 * 输出的k3,v3
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    /**
     * 撰写我们自己的reduce 方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum =0;
        for (IntWritable value : values) {
            int i=value.get();
            sum+=i;
        }
        context.write(key,new IntWritable(sum));

    }
}
