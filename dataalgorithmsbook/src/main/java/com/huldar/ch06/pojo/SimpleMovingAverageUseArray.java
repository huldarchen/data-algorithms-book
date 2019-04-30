package com.huldar.ch06.pojo;

/**
 * 移动平均--使用数组实现
 *
 * @author huldar
 * @date 2019/4/29 09:11
 */
public class SimpleMovingAverageUseArray {
    private double sum = 0.0;
    private final int period;
    private double[] window = null;
    private int pointer = 0;
    private int size = 0;

    public SimpleMovingAverageUseArray(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0 ");
        }
        this.period = period;
        this.window = new double[period];
    }

    public void addNewNumber(double number) {
        sum += number;
        if (size < period) {
            window[pointer++] = number;
            size++;
        } else {
            pointer = pointer % period;
            sum -= window[pointer];
            window[pointer++] = number;
        }
    }

    public double getMovingAverage() {
        if (size == 0) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum / size;
    }

}
