package com.zhouq.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN VALUEIN 对于map 阶段输出的KEYOUT VALUEOUT
 * <p>
 * KEYOUT :是自定义 reduce 逻辑处理结果的key
 * VALUEOUT : 是自定义reduce 逻辑处理结果的 value
 */
public class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * <zhouq,1>,<zhouq,1>,<zhouq,2> ......
     * 入参key 是一组单词的kv对 的 key
     */

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //拿到当前传送进来的 单词
        String word = key.toString();

        //
        int count = 0;

        for (IntWritable value : values) {
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }
}
