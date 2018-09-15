package com.fgm.demo7.inputformat;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 *recordReader的核心逻辑
 * 通过nextKeyValue方法读取数据构造将返回的key value
 * @Auther: fgm
 * @Date: 2018/9/3 0:13
 */
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private FileSplit fileSplit;
    private Configuration conf;
    BytesWritable bytesWritable=new BytesWritable();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(conf);
            FSDataInputStream in  = fileSystem.open(path);
            IOUtils.readFully(in, contents, 0, (int)fileSplit.getLength());
            bytesWritable.set(contents,0,(int)fileSplit.getLength());

            IOUtils.closeStream(in);
            fileSystem.close();
            processed=true;
            return true;
        }

        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed?1.0f:0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}
