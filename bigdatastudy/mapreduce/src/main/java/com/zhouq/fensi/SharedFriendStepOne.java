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

/**
 *  A: C,G,S,H,W,F,Y
 *
 *  找出qq 共同好友。第一步 先找出 这个人是谁的的共同好友
 *
 * Created by zq on 2018/12/13.
 */
public class SharedFriendStepOne {

    static class SharedFriendStepOneMapper extends Mapper<LongWritable,Text,Text,Text>{

        //A: C,G,S,H,W,F,Y
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(":");
            String person = split[0];
            String[] frends= split[1].split(",");
            //
            for (String frend : frends) {
                context.write(new Text(frend),new Text(person));
            }
        }
    }

    static class SharedFriendStepOneReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text frend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();

            for (Text person : persons) {
                sb.append(person).append(",");
            }
            context.write(frend,new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendStepOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SharedFriendStepOneMapper.class);
        job.setReducerClass(SharedFriendStepOneReduce.class);

        FileInputFormat.setInputPaths(job, new Path("F:/hadoop-run/mr/friends/"));
        FileOutputFormat.setOutputPath(job, new Path("F:/hadoop-run/mr/friends/out"));

        job.waitForCompletion(true);
    }
}
