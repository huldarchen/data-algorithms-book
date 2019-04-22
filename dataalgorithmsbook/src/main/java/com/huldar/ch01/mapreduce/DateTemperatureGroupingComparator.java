package com.huldar.ch01.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/13 17:11
 */
public class DateTemperatureGroupingComparator extends WritableComparator {

    protected DateTemperatureGroupingComparator(Class<? extends WritableComparable> keyClass) {
        super(keyClass);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        DateTemperaturePair pair1 = (DateTemperaturePair) wc1;
        DateTemperaturePair pair2 = (DateTemperaturePair) wc2;
        return pair1.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
