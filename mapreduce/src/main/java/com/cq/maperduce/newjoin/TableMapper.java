package com.cq.maperduce.newjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TableMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> map = new HashMap<>();

    private Text outK = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException {

        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] split = line.split("\t");
            map.put(split[0], split[1]);
        }
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        String name = map.get(split[1]);
        outK.set(split[0] + "\t" + name + "\t" + split[2]);
        context.write(outK, NullWritable.get());
    }


}
