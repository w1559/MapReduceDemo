package com.cq.maperduce.writablecompare;

import com.cq.maperduce.writable.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);



        FileInputFormat.setInputPaths(job, new Path("E:\\Work\\output\\*"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Work\\foroutput"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
