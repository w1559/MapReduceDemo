package com.cq.maperduce.writablecompare;

import com.cq.maperduce.writable.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowMapper extends Mapper<LongWritable, Text, FlowBean,Text > {
    private FlowBean outK=new FlowBean();
    private Text outV=new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");

        String phone = split[0];

        outV.set(phone);
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow();

        context.write(outK, outV);
    }

}
