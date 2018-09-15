package com.fgm.demo5.commfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/2 21:01
 */
public class CommMapper extends Mapper<LongWritable,Text,Text,Text> {

    Text text=new Text();
    Text user=new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        //获取我们的好友列表
        String[] friendList = split[1].split(",");
        user.set(split[0]);
        for (String friend : friendList) {
            text.set(friend);
            //以我们的每一个好友作为k2,以我们的用户作为v2
            context.write(text,user);
        }

    }
}
