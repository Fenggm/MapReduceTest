package com.fgm.demo4.sortflow;



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
 * @Date: 2018/8/31 19:23
 */
public class SortFlowMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "sortFlow");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\Fenggms\\test\\flow\\output"));
        //2 定义自定义map
        job.setMapperClass(SortFlowMapper.class);
        job.setMapOutputKeyClass(SortFlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //4排序

        //7 自定义reduce
        job.setReducerClass(SortFlowReducer.class);
        job.setMapOutputKeyClass(SortFlowBean.class);
        job.setOutputValueClass(Text.class);

        //8定义输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\Fenggms\\test\\flow\\sortFlowOut"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new SortFlowMain(), args);
        System.exit(run);

    }
}
