package com.huldar.ch04.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/22 09:03
 */
public class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        PairOfStrings outKey = new PairOfStrings();
        PairOfStrings outValue = new PairOfStrings();

        String[] tokens = StringUtils.split(value.toString(), "\t");

        String productId = tokens[1];
        String userId = tokens[2];

        outKey.set(userId, "2");
        outValue.set("p", productId);

        context.write(outKey, outValue);

    }
}
