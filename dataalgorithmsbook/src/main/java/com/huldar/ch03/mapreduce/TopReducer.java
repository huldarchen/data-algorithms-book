package com.huldar.ch03.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/17 09:11
 */
public class TopReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private int topNum = 12;

    private SortedMap<Integer, String> top = new TreeMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.topNum = context.getConfiguration().getInt("topNum", 12);
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String valueAsString = value.toString().trim();
            String[] tokens = valueAsString.split(",");
            String url = tokens[0];
            int frequency = Integer.parseInt(tokens[1]);
            top.put(frequency, url);
            // keep only top N
            if (top.size() > topNum) {
                top.remove(top.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String value : top.values()) {
            context.write(NullWritable.get(), new Text(value));
        }
    }
}
