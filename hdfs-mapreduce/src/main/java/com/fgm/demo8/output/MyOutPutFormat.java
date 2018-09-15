package com.fgm.demo8.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 *定义k3 v3的类型为一行评论的内容,
 * @Auther: fgm
 * @Date: 2018/9/3 20:13
 */
public class MyOutPutFormat extends FileOutputFormat<Text,NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        //拿到一个输出流,专门写出去
        FSDataOutputStream goodStream = fileSystem.create(new Path("file:///D:\\Fenggms\\test\\自定义outputformat\\output_good\\good.txt"));

        FSDataOutputStream badStream = fileSystem.create(new Path("file:///D:\\Fenggms\\test\\自定义outputformat\\outputbad\\bad.txt"));

        MyRecordWriter myRecordWriter = new MyRecordWriter(goodStream, badStream);
        return myRecordWriter;
    }
}
