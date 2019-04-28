package com.huldar.ch04.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/23 09:40
 */
public class LocationCountReducer extends Reducer<Text, Text, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<>();
        for (Text value : values) {
            set.add(value.toString());
        }
        context.write(key, new LongWritable(set.size()));
    }
}
