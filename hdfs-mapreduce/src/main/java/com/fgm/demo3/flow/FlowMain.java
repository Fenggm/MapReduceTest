package com.fgm.demo3.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 15:26
 */
public class FlowMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), "flowJob");
        job.setJarByClass(FlowMain.class);
        //1
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job,new Path("file:///D:\\Fenggms\\test\\flow\\input"));
        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/flowpartition/input"));
        //自定义map逻辑
        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //3分区
        job.setPartitionerClass(FlowPartition.class);
        job.setNumReduceTasks(6);

        //7自定义reduce逻辑
        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //8设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\Fenggms\\test\\flow\\output"));
        TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/flowpartition/outputsnappy"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {

        //配置我们mapper阶段的压缩
        Configuration configuration=new Configuration();
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        //设置我们reduce阶段的压缩
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");


        int run = ToolRunner.run(configuration, new FlowMain(), args);
        System.exit(run);
    }
}
