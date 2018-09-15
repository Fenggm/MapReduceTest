package com.fgm.demo5.commfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/2 22:04
 */
public class Step2Reducer extends Reducer<Text,Text,Text,Text> {

    Text text=new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer=new StringBuffer();
        for (Text value : values) {
            buffer.append(value.toString()).append(" ");
        }
        text.set(buffer.toString());
        context.write(key,text);
    }
}
