package com.fgm.demo3.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 15:26
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean flowBean=new FlowBean();


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Integer upFlow=0;
        Integer downFlow=0;
        Integer upCountFlow=0;
        Integer downCountFlow=0;

        for (FlowBean value : values) {
            upFlow+=value.getUpFlow();
            downFlow+=value.getDownFlow();
            upCountFlow+=value.getUpCountFlow();
            downCountFlow+=value.getDownCountFlow();
        }

        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);

        context.write(key,flowBean);

    }
}
