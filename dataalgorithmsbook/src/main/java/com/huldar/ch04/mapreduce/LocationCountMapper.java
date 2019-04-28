package com.huldar.ch04.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 地址统计mapper
 * 输入的是 上一个应用的输出
 *
 * @author huldar
 * @date 2019/4/23 09:27
 */
public class LocationCountMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
