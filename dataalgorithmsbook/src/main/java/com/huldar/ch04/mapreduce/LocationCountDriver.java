package com.huldar.ch04.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 本地location的启动类
 *
 * @author huldar
 * @date 2019/4/23 09:42
 */
public class LocationCountDriver {
    public static void main(String[] args) throws IOException {
        Path input = new Path(args[0]);
        Path output = new Path(args[0]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置任务启动类及任务名称
        job.setJarByClass(LocationCountDriver.class);
        job.setJobName(LocationCountDriver.class.getSimpleName());
        //设置任务输入路径 及输入的格式
        FileInputFormat.addInputPath(job, input);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        //设置任务mapper阶段的类和输出类型
        job.setMapperClass(LocationCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(LocationCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        try {
            job.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
