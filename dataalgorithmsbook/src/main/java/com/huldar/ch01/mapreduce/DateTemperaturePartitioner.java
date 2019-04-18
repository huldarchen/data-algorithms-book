package com.huldar.ch01.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/13 17:40
 */
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair pair, Text text, int numPartitions) {
        return Math.abs(pair.getYearMonth().hashCode() % numPartitions);
    }
}
