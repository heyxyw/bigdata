package com.zhouq.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Create by zhouq on 2019/1/27
 *
 * Java Lambda 表达式版本的  WordCount
 */
public class JavaLambdaWordCount {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("JavaWordCount");
        //创建SparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定读取数据的位置
        JavaRDD<String> lines = jsc.textFile(args[0]);

        //切分压平
//        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());

        //将单词进行组合 (a,1),(b,1),(c,1),(a,1)
//        words.mapToPair(tp -> new Tuple2<>(tp,1));
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair((PairFunction<String, String, Integer>) tp -> new Tuple2<>(tp, 1));

        //先交换再排序,因为 只有groupByKey 方法
//        swaped.mapToPair(tp -> tp.swap());
        JavaPairRDD<Integer, String> swaped = wordAndOne.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) tp -> {
//                return new Tuple2<>(tp._2, tp._1);
            return tp.swap();
        });


        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);

        //再次交换顺序
//        sorted.mapToPair(tp -> tp.swap());
        JavaPairRDD<String, Integer> result = sorted.mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) tp -> tp.swap());

        //输出到hdfs
        result.saveAsTextFile(args[1]);

        jsc.stop();

    }
}
