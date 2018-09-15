package com.fgm.demo1.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 9:07
 */
public class PartitionerOwn extends Partitioner<Text,NullWritable> {

    /**
     *
     * @param text
     * @param nullWritable
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String result = text.toString().split("\t")[5];
        System.out.println(result);
        if (Integer.parseInt(result)>15){
            return 1;
        }else{
            return 0;
        }
    }
}
