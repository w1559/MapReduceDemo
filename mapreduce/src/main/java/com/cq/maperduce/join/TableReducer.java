package com.cq.maperduce.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> order = new ArrayList<>();
        TableBean pd=new TableBean();
        for (TableBean value : values) {
            if("order".equals(value.getFlag())){
                TableBean temp = new TableBean();
                try {
                    BeanUtils.copyProperties(temp,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                order.add(temp);
            }
            else {
                try {
                    BeanUtils.copyProperties(pd,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (TableBean tableBean : order) {
            tableBean.setPname(pd.getPname());
            context.write(tableBean,NullWritable.get());
        }
    }
}
