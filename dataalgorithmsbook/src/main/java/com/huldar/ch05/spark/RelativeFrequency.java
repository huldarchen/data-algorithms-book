package com.huldar.ch05.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;


/**
 * 反转排序的spark java实现
 *
 * @author huldar
 * @date 2019/4/28 08:47
 */
public class RelativeFrequency {
    private static final Logger LOGGER = LoggerFactory.getLogger(RelativeFrequency.class);

    public static void main(String[] args) {
        //输入参数验证
        if (args.length != 3) {
            LOGGER.error("Usage: RelativeFrequencyJava <neighbor-window> <input-dir> <output-dir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName(RelativeFrequency.class.getSimpleName());
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        int neighborWindow = Integer.parseInt(args[0]);
        String input = args[1];
        String output = args[2];

        Broadcast<Integer> broadWindow = javaSparkContext.broadcast(neighborWindow);

        JavaRDD<String> rawData = javaSparkContext.textFile(input);

        JavaPairRDD<String, Tuple2<String, Integer>> pair = rawData.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, Integer>>() {

            //TODO 为什么要加这个序列化参数
            private static final long serialVersionUID = -6865482272279657516L;

            @Override
            public Iterator<Tuple2<String, Tuple2<String, Integer>>> call(String line) throws Exception {
                List<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<>();
                String[] tokens = line.split("\\s");
                for (int i = 0; i < tokens.length; i++) {
                    int start = (i - broadWindow.value() < 0) ? 0 : i - broadWindow.value();
                    int end = (i + broadWindow.value() >= tokens.length) ? tokens.length - 1 : i + broadWindow.value();
                    for (int j = start; j < end; j++) {
                        if (j != i) {
                            list.add(new Tuple2<>(tokens[i], new Tuple2<>(tokens[j], 1)));
                        } else {
                            continue;
                        }
                    }
                }
                return list.iterator();
            }
        });
        JavaPairRDD<String, Integer> totalByKey = pair.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, Integer>() {
                    private static final long serialVersionUID = -5109813132039457571L;

                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Tuple2<String, Integer>> tuple) throws Exception {
                        return new Tuple2<>(tuple._1, tuple._2._2);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = -3853125483065745513L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return (v1 + v2);
                    }
                }
        );

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> grouped = pair.groupByKey();

        JavaPairRDD<String, Tuple2<String, Integer>> uniquePairs = grouped.flatMapValues(
                new Function<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>() {

                    private static final long serialVersionUID = -6153213174912055798L;

                    @Override
                    public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> values) throws Exception {
                        HashMap<String, Integer> map = new HashMap<>();
                        List<Tuple2<String, Integer>> list = new ArrayList<>();

                        Iterator<Tuple2<String, Integer>> iterator = values.iterator();

                        while (iterator.hasNext()) {
                            Tuple2<String, Integer> value = iterator.next();
                            int total = value._2;
                            if (map.containsKey(value._1)) {
                                total += map.get(value._1);
                            }
                            map.put(value._1, total);
                            for (Map.Entry<String, Integer> kv : map.entrySet()) {
                                list.add(new Tuple2<>(kv.getKey(), kv.getValue()));
                            }
                            return list;
                        }
                        return null;
                    }
                });
        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> joined = uniquePairs.join(totalByKey);

        JavaPairRDD<Tuple2<String, String>, Double> relativeFrequency = joined.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>>, Tuple2<String, String>, Double>() {
                    private static final long serialVersionUID = -727287386409925158L;

                    @Override
                    public Tuple2<Tuple2<String, String>, Double> call(Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> tuple) throws Exception {
                        return new Tuple2<>(new Tuple2<>(tuple._1, tuple._2._1._1), ((double) tuple._2._1._2 / tuple._2._2));
                    }
                }
        );

        JavaRDD<String> formatResultTabSeparated = relativeFrequency.map(
                new Function<Tuple2<Tuple2<String, String>, Double>, String>() {
                    private static final long serialVersionUID = 7312542139027147922L;

                    @Override
                    public String call(Tuple2<Tuple2<String, String>, Double> tuple) throws Exception {
                        return tuple._1._1 + "\t" + tuple._1._2 + "\t" + tuple._2;
                    }
                });

        formatResultTabSeparated.saveAsTextFile(output);

        javaSparkContext.stop();

    }
}

