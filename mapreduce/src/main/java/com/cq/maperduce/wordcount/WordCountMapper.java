package com.cq.maperduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * map输入阶段key的类型   map输入阶段value的类型 map输出阶段key的类型   map输出阶段value的类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text outK=new Text();
    private IntWritable outV=new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            outK.set(word);
            context.write(outK, outV);
        }
    }
}
