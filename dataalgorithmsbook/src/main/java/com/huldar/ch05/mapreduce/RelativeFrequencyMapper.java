package com.huldar.ch05.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/26 09:14
 */
public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, PairOfWords, IntWritable> {
    /**
     * 相邻的窗口 默认是相邻2个
     */
    private int neighborWindow = 2;

    private final PairOfWords pair = new PairOfWords();
    private final IntWritable totalCount = new IntWritable();
    private static final IntWritable ONE = new IntWritable(1);
    private static final int STRING_LENGTH = 2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        neighborWindow = context.getConfiguration().getInt("neighbor.window", 2);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = StringUtils.split(value.toString(), " ");

        if (tokens == null || tokens.length < STRING_LENGTH) {
            return;
        }

        //TODO huldar 处理技巧需要学习下
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].replaceAll("\\W+", "");

            if ("".equals(tokens[i])) {
                continue;
            }

            pair.setWord(tokens[i]);

            int start = (i - neighborWindow < 0) ? 0 : i - neighborWindow;
            int end = (i + neighborWindow >= tokens.length) ? tokens.length - 1 : i + neighborWindow;

            for (int j = start; j < end; j++) {
                if (j == i) {
                    continue;
                }
                pair.setNeighbor(tokens[j].replaceAll("\\W", ""));
                context.write(pair, ONE);
            }
            pair.setNeighbor("*");
            totalCount.set(end - start);
            context.write(pair, totalCount);
        }
    }
}
