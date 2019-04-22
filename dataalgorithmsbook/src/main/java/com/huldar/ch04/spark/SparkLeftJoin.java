package com.huldar.ch04.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * spark的左连接
 *
 * @author huldar
 * @date 2019/4/22 16:34
 */
public class SparkLeftJoin {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: SparkLeftJoin <user> <transactions>");
            System.exit(1);
        }
        String userInpuFile = args[0];
        String transcationsInputFile = args[1];

        SparkConf conf = new SparkConf();
        conf.setAppName(SparkLeftJoin.class.getSimpleName())
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> userLines = javaSparkContext.textFile(userInpuFile, 1);
        JavaRDD<String> transcationsLines = javaSparkContext.textFile(transcationsInputFile, 1);

        JavaPairRDD<String, Tuple2<String, String>> usersRDD = userLines.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                String[] tokens = s.split("\t");
                String userId = tokens[0];
                String locationId = tokens[1];
                Tuple2<String, String> location = new Tuple2<>("L", locationId);
                return new Tuple2<>(userId, location);
            }
        });
        JavaPairRDD<String, Tuple2<String, String>> transactionsRDD = transcationsLines.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
                String[] transactionRecord = s.split("\t");
                Tuple2<String, String> product = new Tuple2<String, String>("P", transactionRecord[1]);
                return new Tuple2<String, Tuple2<String, String>>(transactionRecord[2], product);
            }
        });

        JavaPairRDD<String, Tuple2<String, String>> allRDD = transactionsRDD.union(usersRDD);
        JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRDD = allRDD.groupByKey();
        JavaPairRDD<String, String> productLocationRDD = groupedRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<Tuple2<String, String>>> stringIterableTuple2) throws Exception {
                Iterable<Tuple2<String, String>> pairs = stringIterableTuple2._2;
                String location = "UNKNOWN";
                List<String> products = new ArrayList<>();
                for (Tuple2<String, String> pair : pairs) {
                    if (pair._1.equals("L")) {
                        location = pair._2;
                    } else {
                        products.add(pair._2);
                    }
                }
                List<Tuple2<String, String>> kvList = new ArrayList<>();
                for (String product : products) {
                    kvList.add(new Tuple2<>(product, location));
                }
                return kvList.iterator();
            }
        });
        JavaPairRDD<String, Iterable<String>> productionByLocation = productLocationRDD.groupByKey();

        List<Tuple2<String, Iterable<String>>> collect = productionByLocation.collect();
        for (Tuple2<String, Iterable<String>> tuple : collect) {
            System.out.println(tuple._1);
            System.out.println(tuple._2);
        }

        JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocations = productionByLocation.mapValues(new Function<Iterable<String>, Tuple2<Set<String>, Integer>>() {
            @Override
            public Tuple2<Set<String>, Integer> call(Iterable<String> s) throws Exception {
                Set<String> uniqueLocations = new HashSet<String>();
                for (String location : s) {
                    uniqueLocations.add(location);
                }
                return new Tuple2<Set<String>, Integer>(uniqueLocations, uniqueLocations.size());

            }
        });
        // debug4
        System.out.println("=== Unique Locations and Counts ===");
        List<Tuple2<String, Tuple2<Set<String>, Integer>>> debug4 = productByUniqueLocations.collect();
        System.out.println("--- debug4 begin ---");
        for (Tuple2<String, Tuple2<Set<String>, Integer>> t2 : debug4) {
            System.out.println("debug4 t2._1=" + t2._1);
            System.out.println("debug4 t2._2=" + t2._2);
        }
        System.out.println("--- debug4 end ---");
        productByUniqueLocations.saveAsTextFile("/left/output");
        System.exit(0);

    }

}
