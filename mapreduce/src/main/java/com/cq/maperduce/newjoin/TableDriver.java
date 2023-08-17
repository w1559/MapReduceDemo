package com.cq.maperduce.newjoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TableDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf=new Configuration();

        //map输入压缩
        conf.setBoolean("mapreduce.map.output.compress",true);
        //map输入压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        Job job= Job.getInstance(conf);

        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,new Path("E:\\Work\\table\\order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Work\\tableOutNew"));
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);

        job.addCacheFile(new URI("file:///E:/Work/cache/pd.txt"));

        job.setNumReduceTasks(0);

        boolean result=job.waitForCompletion(true);

        System.exit(result? 0 : 1);
    }


}
