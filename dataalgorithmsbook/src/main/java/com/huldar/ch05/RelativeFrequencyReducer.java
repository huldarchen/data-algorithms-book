package com.huldar.ch05;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer端进行聚合 求出上下文的频率
 *
 * @author huldar
 * @date 2019/4/28 08:28
 */
public class RelativeFrequencyReducer extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {
    /**
     * 总数
     */
    private double totalCount = 0;
    private final DoubleWritable relativeCount = new DoubleWritable();
    private String currentWord = "NOT_DEFINED";

    private static final String START_TAG = "*";


    @Override
    protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //计算总的数量 通过 * 来的来计算

        if (START_TAG.equals(key.getNeighbor())) {
            if (currentWord.equals(key.getWord())) {
                totalCount += totalCount + getTotalCount(values);
            } else {
                currentWord = key.getWord();
                totalCount += getTotalCount(values);
            }
        } else {
            double count = getTotalCount(values);
            relativeCount.set(count / totalCount);
            context.write(key, relativeCount);
        }

    }

    /**
     * 计算总的数量
     *
     * @param values
     * @return
     */
    private double getTotalCount(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        return sum;
    }
}
