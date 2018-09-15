package com.fgm.demo8.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 *自定义recordWriter,最终就是这个writer方法往外写数据
 * @Auther: fgm
 * @Date: 2018/9/3 20:21
 */
public class MyRecordWriter extends RecordWriter<Text,NullWritable> {

    private FSDataOutputStream goodStream;
    private FSDataOutputStream badStream;

    public MyRecordWriter(){}

    public MyRecordWriter(FSDataOutputStream goodStream, FSDataOutputStream badStream) {
        this.badStream=badStream;
        this.goodStream=goodStream;
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        //split
        String[] split = text.toString().split("\t");
        String commentStatus = split[9];
        if (Integer.parseInt(commentStatus)<=1){
            goodStream.write(text.getBytes());
            goodStream.write("\r\n".getBytes());
        }else{
            badStream.write(text.getBytes());
            badStream.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        IOUtils.closeStream(goodStream);
        IOUtils.closeStream(badStream);
    }
}
