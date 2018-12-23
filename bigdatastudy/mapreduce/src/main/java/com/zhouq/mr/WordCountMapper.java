package com.zhouq.mr;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN 默认情况下，是MR 框架中读取到的一行文本的起始偏移量，long 类型
 * 在hadoop 中有自己更精简的序列化接口，我们不直接用Long ，而是用 LongWritable
 * VALUEIN : 默认情况下，是MR 中读取到的一行文本内容，String ，也有自己的类型 Text 类型
 * <p>
 * KEYOUT ： 是用户自定义的逻辑处理完成后的自定义输出数据的key ,我们这里是单词，类型为string 同上，Text
 * <p>
 * VALUEOUT： 是用户自定义的逻辑处理完成后的自定义输出value 类型，我们这里是单词数量Integer,同上，Integer 也有自己的类型 IntWritable
 * <p>
 * <p>
 * <p>
 * <p>
 * Created by zq on 2018/12/10.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * map 阶段的业务逻辑就写在map 方法内
     * maptask 会对每一行输入数据 就调用一次我们自定义的map 方法。
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //拿到输入的这行数据
        String line = value.toString();

        //根据空格进行分割得到这行的单词
        String[] words = line.split(" ");

        //将单词输出为 <word,1>
        for (String word : words) {
            //将单词作为key ，将次数 做为value输出，
            // 这样也利于后面的数据分发，可以根据单词进行分发，
            // 以便于相同的单词落到相同的reduce task 上,方便统计

            context.write(new Text(word), new IntWritable(1));
        }

    }
}
