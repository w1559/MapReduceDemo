package com.cq.maperduce.telcount;

import com.cq.maperduce.writable.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private FlowBean outV=new FlowBean();
    private Text outK=new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phone = split[1];

        outK.set(phone);
        outV.setUpFlow(Long.parseLong(split[split.length - 3]));
        outV.setDownFlow(Long.parseLong(split[split.length - 2]));
        outV.setSumFlow();
        context.write(outK,outV);
    }
}