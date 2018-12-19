package com.zhouq.fensi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 *  找出qq 共同好友。第一步已经找出来 当前这个人都是那些人的好友 c  a,d,g,e,h,t
 *  现在再去按照好友切分 两两组合，做成 <a-d,c> <a-g,c> ....  再reduce 端进行合并  就能知道 a-d 的共同好友是谁。。
 *
 */
public class SharedFriendsStepTwo {


    static class SharedFriendsStepTwoMapper extends Mapper<LongWritable,Text,Text,Text>{

        //那到上一个阶段的好友数据  c a,d,g,e,h,t
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String friend = split[0];
            String[] persions = split[1].split(",");
            //先对persions 数据进行排序 ，避免出现 a-b ， b-a 的情况
            Arrays.sort(persions);
            for (int i = 0; i < persions.length -1; i++){
                for (int j = 1 ;j < persions.length ;j++){
                    //剔除自己匹配自己的情况
                    if (!persions[i].equals(persions[j])){
                        //输出 <a-d,c>  <a-g,c> ..... 相同的 <人-人，好友>  会发送到同一个reduce 上面去。
                        context.write(new Text(persions[i] + "-" + persions[j]),new Text(friend));
                    }
                }
            }

        }
    }

    static class SharedFriendsStepTwoReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text persion_persion, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : friends) {
                sb.append(value.toString()).append(",");
            }

            context.write(persion_persion,new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendsStepTwo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SharedFriendsStepTwoMapper.class);
        job.setReducerClass(SharedFriendsStepTwoReduce.class);

        FileInputFormat.setInputPaths(job, new Path("F:/hadoop-run/mr/friends/out/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("F:/hadoop-run/mr/friends/out3"));

        job.waitForCompletion(true);
    }
}
