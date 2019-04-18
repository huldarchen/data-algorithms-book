package com.huldar.ch03.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/17 09:02
 */
public class TopMapper extends Mapper<Text, IntWritable, NullWritable, Text> {
    private int topNum = 12;
    private SortedMap<Integer, String> top = new TreeMap<>();

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String keyAsString = key.toString();
        int frequency = value.get();
        String compositeValue = keyAsString + "," + frequency;
        top.put(frequency, compositeValue);

        if (top.size() > topNum) {
            top.remove(top.firstKey());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.topNum = context.getConfiguration().getInt("topNum", 12);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String value : top.values()) {
            context.write(NullWritable.get(), new Text(value));

        }
    }
}
