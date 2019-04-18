package com.huldar.ch02.spark;

import com.huldar.util.SparkUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/18 09:10
 */
public class SparkTopWithOutInBuiltFunction {

    public static void main(String[] args) throws Exception {
        //确定参数的合法性
        if (args.length < 1) {
            System.err.println("Usage: Top10 <input-file>");
            System.exit(1);
        }

        String inputPath = args[0];
        System.out.println("args[0]: <input-path>=" + inputPath);

        JavaSparkContext jsc = SparkUtil.createJavaSparkContext(SparkTopWithOutInBuiltFunction.class.getSimpleName());

        JavaRDD<String> lines = jsc.textFile(inputPath);

        JavaPairRDD<String, Integer> castAndWeigth = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] tokens = line.split(",");
                return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
            }
        });


        //executor端的topN
        JavaRDD<SortedMap<Integer, String>> sortedMapJavaRDD = castAndWeigth.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {
            @Override
            public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {

                SortedMap<Integer, String> executorTop = new TreeMap<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Integer> tuple = tuple2Iterator.next();
                    executorTop.put(tuple._2, tuple._1);
                    if (executorTop.size() > 10) {
                        executorTop.remove(executorTop.firstKey());
                    }
                }
                return Collections.singletonList(executorTop).iterator();
            }
        });


        //最终topN
        SortedMap<Integer,String> finalTop = new TreeMap<>();
        List<SortedMap<Integer, String>> executorTop = sortedMapJavaRDD.collect();
        for (SortedMap<Integer, String> sortedMap : executorTop) {
            for (Map.Entry<Integer, String> integerStringEntry : sortedMap.entrySet()) {
                finalTop.put(integerStringEntry.getKey(),integerStringEntry.getValue());
                if (finalTop.size() >10 ){
                    finalTop.remove(finalTop.firstKey());
                }
            }
        }

        for (Map.Entry<Integer, String> entry : finalTop.entrySet()) {
            System.out.println(entry.getKey() + "--" + entry.getValue());
        }


        jsc.stop();

    }
}
