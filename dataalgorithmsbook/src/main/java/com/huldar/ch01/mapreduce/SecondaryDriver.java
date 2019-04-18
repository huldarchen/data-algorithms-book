package com.huldar.ch01.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/13 17:54
 */
public class SecondaryDriver extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecondaryDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondaryDriver.class);
        job.setJobName(SecondaryDriver.class.getSimpleName());


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondaryReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setSortComparatorClass(DateTemperatureGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        LOGGER.info("run():status={}", status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            LOGGER.warn("SecondarySortDriver <input-dir> <output-dir>");
            throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output-dir");
        }
        int returnStatus = submitJob(args);

        LOGGER.info("returnStatus = {}", returnStatus);

        System.exit(returnStatus);
    }

    private static int submitJob(String[] args) throws Exception {
        return ToolRunner.run(new SecondaryDriver(), args);
    }

}
