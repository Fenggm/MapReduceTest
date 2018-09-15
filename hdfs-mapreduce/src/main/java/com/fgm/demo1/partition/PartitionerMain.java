package com.fgm.demo1.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 9:00
 */
public class PartitionerMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), PartitionerMain.class.getSimpleName());
        job.setJarByClass(PartitionerMain.class);
        //1 读取文件,解析成k v对
        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job,new Path(args[0]));
        //2 自定义map逻辑,接收k1 v1
        job.setMapperClass(PartitionerMapper.class);
        // 设置k2 v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //3 分区
        job.setPartitionerClass(PartitionerOwn.class);
        //设置我们的分区个数为两个
        job.setNumReduceTasks(2);

        //7.自定义reduce逻辑,接收k2 v2 ,输出k3 v3
        job.setReducerClass(PartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //8 设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(args[1]));

        //设置job等待时间
        boolean b = job.waitForCompletion(true);
        return b?0:1;

    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new PartitionerMain(), args);
        System.exit(run);

    }
}
