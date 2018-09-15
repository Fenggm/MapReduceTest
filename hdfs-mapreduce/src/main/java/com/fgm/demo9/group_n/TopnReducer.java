package com.fgm.demo9.group_n;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/3 21:07
 */
public class TopnReducer extends Reducer<OrderBean, Text, OrderBean, Text> {

    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int i=0;
        for (Text value : values) {
            context.write(key,value);
            i++;
            if (i>=2){
                break;
            }

        }
    }
}
