package com.fgm.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *通过mr的编程模板设置主类
 * @Auther: fgm
 * @Date: 2018/8/31 0:12
 */
public class WordCountMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), "xxxx");

        job.setJarByClass(WordCountMain.class);

        //第一步 读取文件,解析成key,value 对

        job.setInputFormatClass(TextInputFormat.class);
        //设置我们读取的路径
        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/wordcount/input"));
        //第二步 自定义map逻辑,接收我们的k1 v1 ,转换成k2 v2输出

        job.setMapperClass(WordCountMapper.class);
        //设置k2 v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //第三步 到第六步 分区 排序 规约 分组

        //第七步 自定义reduce逻辑,接收我们的k2,v2输出成k3,v3

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //第8步,设置我们输出的目的地
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://node01:8020/wordcount/output"));

        //设置我们的job任务等待退出
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }



    public static void main(String[] args) throws Exception {

        //程序执行的最终结果
        int run = ToolRunner.run(new Configuration(), new WordCountMain(), args);

        //判断执行结果
        System.exit(run);
    }

}
