package com.huldar.ch03.spark;

import com.huldar.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * 使用spark的takeOrdered函数获取键非唯一的Top10
 *
 * @author huldar
 * @date 2019/4/19 09:10
 */
public class Top10UsingTakeOrdered {
    public static void main(String[] args) throws Exception {
        // 处理参数
        //1输入参数处理
        if (args.length < 2) {
            System.err.println("Usage: Top10NonUnique <input-path> <TopN>");
        }
        final String inputPath = args[0];
        final int topNum = Integer.parseInt(args[1]);
        // 创建SparkContext
        JavaSparkContext javaSparkContext = SparkUtil.createJavaSparkContext(SparkTopNonUnique.class.getSimpleName());
        // 从输入获取RDD
        JavaRDD<String> lines = javaSparkContext.textFile(inputPath);
        // RDD分区
        JavaRDD<String> coalesceRDD = lines.coalesce(9);

        // 输入T映射到(k,v)
        JavaPairRDD<String, Integer> kv = coalesceRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] tokens = line.split(",", -1);
                return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
            }
        });
        //规约重复的k
        JavaPairRDD<String, Integer> uniqueKeys = kv.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 调用takeOrdered() 查找最终 topN
        List<Tuple2<String, Integer>> finalTop = uniqueKeys.takeOrdered(topNum, MyTupleComparator.INSTANCE);
        // takeOrdered替换方案 使用top函数
        for (Tuple2<String, Integer> tuple2 : finalTop) {
            System.out.println(tuple2._1 + "," + tuple2._2);
        }

        javaSparkContext.stop();

    }

    static class MyTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

        final static MyTupleComparator INSTANCE = new MyTupleComparator();

        @Override
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t1._2.compareTo(t2._2);
        }

    }


}
