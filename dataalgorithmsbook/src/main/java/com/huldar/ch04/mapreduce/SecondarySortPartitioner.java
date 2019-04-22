package com.huldar.ch04.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/22 09:12
 */
public class SecondarySortPartitioner extends Partitioner<PairOfStrings, Object> {
    @Override
    public int getPartition(PairOfStrings pairOfStrings, Object o, int numPartitions) {
        return (pairOfStrings.getLeftElement().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
