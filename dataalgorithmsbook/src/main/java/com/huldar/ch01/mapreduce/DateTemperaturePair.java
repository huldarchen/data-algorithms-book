package com.huldar.ch01.mapreduce;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import javax.validation.constraints.NotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/13 17:13
 */
public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {

    private final Text yearMonth = new Text();
    private final Text day = new Text();
    private final IntWritable temperature = new IntWritable();

    public DateTemperaturePair() {
    }

    public DateTemperaturePair(String yearMonth, String day, Integer temperature) {
        this.yearMonth.set(yearMonth);
        this.day.set(day);
        this.temperature.set(temperature);
    }

    public static DateTemperaturePair read(DataInput in) throws IOException {
        DateTemperaturePair pair = new DateTemperaturePair();
        pair.readFields(in);
        return pair;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.yearMonth.readFields(in);
        this.day.readFields(in);
        this.temperature.readFields(in);

    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.yearMonth.write(out);
        this.day.write(out);
        this.temperature.write(out);
    }

    @Override
    public int compareTo(@NotNull DateTemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if (compareValue == 0) {
            compareValue = this.temperature.compareTo(pair.getTemperature());
        }
        return compareValue;
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public Text getDay() {
        return day;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth.set(yearMonth);
    }

    public void setDay(String day) {
        this.day.set(day);
    }

    public void setTemperature(Integer temperature) {
        this.temperature.set(temperature);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DateTemperaturePair that = (DateTemperaturePair) o;

        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
            return false;
        }
        return (day != null ? day.equals(that.day) : that.day == null) && (temperature != null ? temperature.equals(that.temperature) : that.temperature == null);
    }

    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;
        result = 31 * result + (day != null ? day.hashCode() : 0);
        result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DateTemperaturePair").append('{')
                .append("yearMonth=")
                .append(yearMonth)
                .append(",day=")
                .append(day)
                .append(",temperature=")
                .append(temperature)
                .append('}');
        return sb.toString();
    }
}
