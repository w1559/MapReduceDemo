package com.cq.maperduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;
    private Text outK=new Text();
    private TableBean outV=new TableBean();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if(fileName.contains("order")){
            String[] s = line.split("\t");
            outK.set(s[1]);
            outV.setId(s[0]);
            outV.setPid(s[1]);
            outV.setAmount(Integer.parseInt(s[2]));
            outV.setPname("");
            outV.setFlag("order");
        }else{
            String[] s = line.split("\t");
            outK.set(s[0]);

            outV.setId("");
            outV.setPid(s[0]);
            outV.setAmount(0);
            outV.setPname(s[1]);
            outV.setFlag("pd");
        }
        context.write(outK,outV);
    }
}
