package com.huldar.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/15 10:53
 */
public class SparkUtil {
    /**
     * Create a JavaSparkContext that loads settings from system properties
     * (for instance, when launching with ./bin/spark-submit).
     *
     * @return a JavaSparkContext
     */
    public static JavaSparkContext createJavaSparkContext()
            throws Exception {
        return new JavaSparkContext();
    }

    /**
     * Create a JavaSparkContext object from a given Spark's master URL
     *
     * @param sparkMasterURL  Spark master URL as "spark://<spark-master-host-name>:7077"
     * @param applicationName application name
     * @return a JavaSparkContext
     */
    public static JavaSparkContext createJavaSparkContext(String sparkMasterURL, String applicationName)
            throws Exception {
        JavaSparkContext ctx = new JavaSparkContext(sparkMasterURL, applicationName);
        return ctx;
    }


    /**
     * Create a JavaSparkContext that loads settings from system properties
     * (for instance, when launching with ./bin/spark-submit).
     *
     * @param applicationName application name
     * @return a JavaSparkContext
     */
    public static JavaSparkContext createJavaSparkContext(String applicationName)
            throws Exception {
        SparkConf conf = new SparkConf().setAppName(applicationName);
        conf.setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

    public static String version() {
        return "2.1.0";
    }
}
