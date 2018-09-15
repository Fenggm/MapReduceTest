package com.fgm.demo2.sort;

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
 *
 * @Auther: fgm
 * @Date: 2018/8/31 9:56
 */
public class SortMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), SortMain.class.getSimpleName());
        //1
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\Fenggms\\test\\sort\\input"));
        //2自定义map逻辑
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(SortPojo.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5设置规约的类,(输入和输出都是k2 v2)
        job.setCombinerClass(CombinerMine.class);

        //7自动以reducer
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //8
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\Fenggms\\test\\sort\\output"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new SortMain(), args);
        System.exit(run);
    }
}
