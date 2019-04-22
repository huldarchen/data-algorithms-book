package com.huldar.ch04.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import java.io.IOException;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/22 09:15
 */
public class SecondarySortGroupComparator implements RawComparator<PairOfStrings> {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        DataInputBuffer buffer = new DataInputBuffer();
        PairOfStrings a = new PairOfStrings();
        PairOfStrings b = new PairOfStrings();

        try {
            buffer.reset(b1, s1, l1);
            a.readFields(buffer);
            buffer.reset(b2, s2, l2);
            b.readFields(buffer);
            return compare(a, b);
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public int compare(PairOfStrings first, PairOfStrings second) {
        return first.getLeftElement().compareTo(second.getLeftElement());
    }
}
