package com.huldar.ch01.sparkwithlambda;

import com.huldar.util.DataStructures;
import com.huldar.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/15 10:50
 */
public class SecondarySortUsingCombineByKey {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SecondarySortUsingCombineByKey <input> <output>");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        JavaSparkContext sparkContext = SparkUtil.createJavaSparkContext();

        JavaRDD<String> line = sparkContext.textFile(inputPath);

        JavaPairRDD<String, Tuple2<Integer, Integer>> pair = line.mapToPair((String s) -> {
            String[] tokens = s.split(",", -1);
            Tuple2<Integer, Integer> timevalue = new Tuple2<>(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
            return new Tuple2<>(tokens[0], timevalue);
        });
        List<Tuple2<String, Tuple2<Integer, Integer>>> collect = pair.collect();
        for (Tuple2<String, Tuple2<Integer, Integer>> timeValue : collect) {
            Tuple2<Integer, Integer> timevalue = (Tuple2<Integer, Integer>) timeValue._2;
            System.out.println(timeValue._1 + "," + timevalue._1 + "," + timevalue._1);
        }


        Function<Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> createCombiner
                = (Tuple2<Integer, Integer> x) -> {
            Integer time = x._1;
            Integer value = x._2;
            SortedMap<Integer, Integer> map = new TreeMap<>();
            map.put(time, value);
            return map;
        };

        Function2<SortedMap<Integer, Integer>, Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> mergeValue
                = (SortedMap<Integer, Integer> map, Tuple2<Integer, Integer> x) -> {
            Integer time = x._1;
            Integer value = x._2;
            map.put(time, value);
            return map;
        };

        Function2<SortedMap<Integer, Integer>, SortedMap<Integer, Integer>, SortedMap<Integer, Integer>> mergeCombiners
                = (SortedMap<Integer, Integer> map1, SortedMap<Integer, Integer> map2) -> {
            if (map1.size() < map2.size()) {
                return DataStructures.merge(map1, map2);
            } else {
                return DataStructures.merge(map1, map2);
            }
        };

        JavaPairRDD<String, SortedMap<Integer, Integer>> combined = pair.combineByKey(
                createCombiner,
                mergeValue,
                mergeCombiners
        );

        List<Tuple2<String, SortedMap<Integer, Integer>>> output2 = combined.collect();
        for (Tuple2<String, SortedMap<Integer, Integer>> t : output2) {
            String name = t._1;
            SortedMap<Integer, Integer> map = t._2;
            System.out.println(name);
            System.out.println(map);
        }

        combined.saveAsTextFile(outputPath);

        sparkContext.close();

        System.exit(0);

    }
}
