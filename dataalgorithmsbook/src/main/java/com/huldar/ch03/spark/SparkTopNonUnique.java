package com.huldar.ch03.spark;

import com.huldar.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * 键非唯一的TopN 方案
 * 例如URL的访问topN
 *
 * @author huldar
 * @date 2019/4/19 08:38
 */
public class SparkTopNonUnique {
    public static void main(String[] args) throws Exception {
        //1输入参数处理
        if (args.length < 2) {
            System.err.println("Usage: Top10NonUnique <input-path> <TopN>");
        }
        final String inputPath = args[0];
        final int topNum = Integer.parseInt(args[1]);

        //2创建java Spark上下文对象
        JavaSparkContext javaSparkContext = SparkUtil.createJavaSparkContext(SparkTopNonUnique.class.getSimpleName());

        //3将TopN广播到所有集群节点
        final Broadcast<Integer> topBroadcast = javaSparkContext.broadcast(topNum);
        //4从输入创建一个RDD
        JavaRDD<String> lines = javaSparkContext.textFile(inputPath);
        /**
         * 进行重分区,重分区数量 经验值为 2*number_executor*cores_per_executor
         */
        //5RDD分区
        JavaRDD<String> coalesceRdd = lines.coalesce(9);
        //6输入(T)映射到(K,V)
        JavaPairRDD<String, Integer> kv = coalesceRdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] tokens = line.split(",", -1);

                return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
            }
        });
        //7 规约重复的K
        JavaPairRDD<String, Integer> uniqueKeys = kv.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //8 创建本地TopN
        JavaRDD<SortedMap<Integer, String>> localSortedMapJavaRDD = uniqueKeys.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {

            @Override
            public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                final int topN = topBroadcast.getValue();
                SortedMap<Integer, String> localTopMap = new TreeMap<>();
                while (iterator.hasNext()) {
                    Tuple2<String, Integer> tuple = iterator.next();
                    localTopMap.put(tuple._2, tuple._1);
                    if (localTopMap.size() > topN) {
                        localTopMap.remove(localTopMap.firstKey());
                    }

                }
                return Collections.singletonList(localTopMap).iterator();
            }
        });


        //9 查找最终TopN
        List<SortedMap<Integer, String>> allTopN = localSortedMapJavaRDD.collect();
        SortedMap<Integer, String> finalTopMap = new TreeMap<>();

        for (SortedMap<Integer, String> localMap : allTopN) {
            for (Map.Entry<Integer, String> entry : localMap.entrySet()) {
                finalTopMap.put(entry.getKey(), entry.getValue());
                if (finalTopMap.size() > topNum) {
                    finalTopMap.remove(finalTopMap.firstKey());
                }
            }
        }

        //10 发出最终TopN
        for (Map.Entry<Integer, String> entry : finalTopMap.entrySet()) {
            System.out.println("key=" + entry.getValue() + " value=" + entry.getKey());
        }

        javaSparkContext.stop();
    }
}
