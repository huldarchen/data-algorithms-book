package com.huldar.ch01.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/13 17:43
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text> {

    private final Text theTemperature = new Text();
    private final DateTemperaturePair pair = new DateTemperaturePair();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //TODO 可以进行过滤
        String[] tokens = line.split(",", -1);
        String yearMonth = tokens[0] + tokens[1];
        String day = tokens[2];
        Integer temperature = Integer.parseInt(tokens[3]);

        pair.setYearMonth(yearMonth);
        pair.setDay(day);
        pair.setTemperature(temperature);
        theTemperature.set(tokens[3]);


        context.write(pair, theTemperature);
    }
}
