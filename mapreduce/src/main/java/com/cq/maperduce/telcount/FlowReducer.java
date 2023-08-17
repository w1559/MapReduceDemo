package com.cq.maperduce.telcount;

import com.cq.maperduce.writable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outV=new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        long up = 0;
        long down = 0;
        for (FlowBean value : values) {
            up+=value.getUpFlow();
            down+=value.getDownFlow();
        }
        outV.setUpFlow(up);
        outV.setDownFlow(down);
        outV.setSumFlow();

        context.write(key,outV);
    }
}