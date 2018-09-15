package com.fgm.demo4.sortflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *接收我们的k2 v2,不做任何处理,直接输出即可
 * @Auther: fgm
 * @Date: 2018/8/31 19:24
 */
public class SortFlowReducer extends Reducer<SortFlowBean,Text,SortFlowBean,Text> {

    @Override
    protected void reduce(SortFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key,value);
        }
    }
}
