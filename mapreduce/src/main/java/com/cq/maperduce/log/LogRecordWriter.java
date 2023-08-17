package com.cq.maperduce.log;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream log;
    private FSDataOutputStream other;

    public LogRecordWriter(TaskAttemptContext job) {
        try{
            FileSystem fs=FileSystem.get(job.getConfiguration());
            log = fs.create(new Path("E:\\Work\\logoutput\\log.log"));
            other = fs.create(new Path("E:\\Work\\logoutput\\other.log"));
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String web=text.toString();
        if(web.contains("cq")){
            log.writeBytes(web+"\n");
        }
        else {
            other.writeBytes(web+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStreams(log,other);
    }
}
