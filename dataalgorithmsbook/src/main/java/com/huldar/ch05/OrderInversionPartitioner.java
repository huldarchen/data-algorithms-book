package com.huldar.ch05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 反转排序的分区器
 *
 * @author huldar
 * @date 2019/4/26 08:43
 */
public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {

    @Override
    public int getPartition(PairOfWords key, IntWritable value, int numberOfPartitions) {
        // key = (leftWord, rightWord) = (word, neighbor)
        String leftWord = key.getLeftElement();
        return Math.abs(((int) hash(leftWord)) % numberOfPartitions);
    }

    private static long hash(String str) {
        // 质数
        long h = 1125899906842597L;
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }
}
