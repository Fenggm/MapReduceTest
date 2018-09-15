package com.fgm.demo5.commfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/2 22:04
 */
public class Step2Mapper extends Mapper<LongWritable,Text,Text,Text> {

    Text textkey=new Text();
    Text textvalue=new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String[] friends = split[0].split("-");
        textvalue.set(split[1]);
        Arrays.sort(friends);
        for (int i=0;i<friends.length-1;i++){
            for (int j=i+1;j<friends.length;j++){
                textkey.set(friends[i]+"-"+friends[j]);
                context.write(textkey,textvalue);
            }
        }
    }
}
