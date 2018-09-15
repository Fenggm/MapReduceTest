package com.fgm.demo7.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**自定义我们的map类,将我们的文件进行输出,key2是我们的文件名称,value2是我们的字节数组
 * @Auther: fgm
 * @Date: 2018/9/3 0:52
 */
public class InputMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    Text text=new Text();
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit=(FileSplit)context.getInputSplit();
        String name = fileSplit.getPath().getName();
        text.set(name);
        context.write(text,value);
    }
}
