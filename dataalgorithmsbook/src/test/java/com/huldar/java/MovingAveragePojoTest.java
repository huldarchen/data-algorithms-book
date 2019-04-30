package com.huldar.java;

import com.huldar.ch06.pojo.SimpleMovingAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用数组和队列实现的移动平均数的测试
 *
 * @author huldar
 * @date 2019/4/29 09:19
 */
public class MovingAveragePojoTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MovingAveragePojoTest.class);

    public static void main(String[] args) {
        //对应OneNote的数据
        double[] testData = {10, 18, 20, 30, 24, 33, 27};
        int[] allWindowSize = {3, 4};
        for (int windowSize : allWindowSize) {
            SimpleMovingAverage simpleMovingAverage = new SimpleMovingAverage(windowSize);
            LOGGER.info("windowSize ={}", simpleMovingAverage);
            for (double data : testData) {
                simpleMovingAverage.addNewNumber(data);
                LOGGER.info("Next number = {},SMA = {}", data, simpleMovingAverage.getMovingAverage());
            }
            LOGGER.info("--------------------------------------------------------");
        }
    }
}
