package com.huldar.ch04.mapreduce;


import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.File;
import java.io.IOException;


/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/22 15:09
 */
public class LeftJoinDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("请输入参数");
            System.exit(1);
        }
        Path transaction = new Path(args[0]);
        Path user = new Path(args[1]);
        Path output = new Path(args[2]);
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(LeftJoinDriver.class);
        job.setJobName("phase-1: Left Outer Join");

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
        job.setSortComparatorClass(PairOfStrings.Comparator.class);

        job.setReducerClass(LeftJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        MultipleInputs.addInputPath(job, transaction, TextInputFormat.class, LeftJoinTransactionMapper.class);
        MultipleInputs.addInputPath(job, user, TextInputFormat.class, LeftJoinUserMapper.class);

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        FileOutputFormat.setOutputPath(job, output);

        try {
            boolean b = job.waitForCompletion(true);
        } catch (IOException e) {
            FileUtil.fullyDelete(new File(args[3]));
            e.printStackTrace();
        } catch (InterruptedException e) {
            FileUtil.fullyDelete(new File(args[3]));
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            FileUtil.fullyDelete(new File(args[3]));
            e.printStackTrace();
        }
    }
}
