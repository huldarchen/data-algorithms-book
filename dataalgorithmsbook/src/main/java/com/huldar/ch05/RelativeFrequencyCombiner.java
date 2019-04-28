package com.huldar.ch05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 中间聚合 接收mapper的数据进行预聚合.(在reduce聚合前进行聚合)
 * 接收的数据是 (pairOfWords,次数),输出的数据是(pairOfWord,次数+)
 *
 * @author huldar
 * @date 2019/4/28 08:21
 */
public class RelativeFrequencyCombiner extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable> {

    @Override
    protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int partialSum = 0;
        for (IntWritable value : values) {
            partialSum += value.get();
        }
        context.write(key, new IntWritable(partialSum));
    }
}
