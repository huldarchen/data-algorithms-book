package com.huldar.ch04.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 用户左连接的实现
 * 输出结果是<(user_id,"1")>-<("L",location_id)>
 * @author huldar
 * @date 2019/4/22 08:50
 */
public class LeftJoinUserMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
    /**
     * @param key     文本偏移量
     * @param value   输入的内容  <user_id>TAB<location_id>
     * @param context MapReduce上下文
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        PairOfStrings outKey = new PairOfStrings();
        PairOfStrings outValue = new PairOfStrings();

        String[] tokens = StringUtils.split(value.toString(), "\t");
        if (tokens.length == 2) {
            outKey.set(tokens[0], "1");
            outValue.set("l", tokens[1]);
            context.write(outKey, outValue);
        }

    }
}
