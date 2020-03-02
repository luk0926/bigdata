package com.kaikeba;

import javafx.scene.shape.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.kaikeba
 * @Author: luk
 * @CreateTime: 2020/1/14 14:55
 */
public class WordCountJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCountJava");
        conf.setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> file = javaSparkContext.textFile("C:\\Users\\JIGUANG\\Desktop\\word.txt");

        JavaRDD<String> stringJavaRDD = file.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(" ");
                Iterator<String> iterator = Arrays.asList(words).iterator();
                return iterator;
            }
        });

        JavaPairRDD<String, Integer> wordAndOne = stringJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> result = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> reverseJavaRDD = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2, t._1);
            }
        });

        JavaPairRDD<String, Integer> sortedRdd = reverseJavaRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2, t._1);
            }
        });

        List<Tuple2<String, Integer>> list = sortedRdd.collect();

        for (Tuple2<String, Integer> t : list) {
            System.out.println(t._1 + "  " + t._2);
        }


        javaSparkContext.stop();
    }
}
