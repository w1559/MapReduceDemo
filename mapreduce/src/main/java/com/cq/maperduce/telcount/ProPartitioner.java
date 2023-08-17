package com.cq.maperduce.telcount;

import com.cq.maperduce.writable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phone=text.toString();

        String pre = phone.substring(0, 3);

        int partition;
        if("136".equals(pre)){
            partition=0;
        }else if("137".equals(pre)){
            partition=1;
        }else if("138".equals(pre)){
            partition=2;
        }else{
            partition=3;
        }

        return partition;
    }
}
