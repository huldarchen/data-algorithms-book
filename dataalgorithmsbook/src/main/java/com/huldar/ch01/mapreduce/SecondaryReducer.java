package com.huldar.ch01.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/13 17:51
 */
public class SecondaryReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {
    @Override
    protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString())
                    .append(",");
        }
        context.write(key.getYearMonth(), new Text(builder.toString()));
    }
}
