package com.zhouq.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // mapreduce.framework.name 配置成 local 就是本地运行模式,默认就是local
        // 所谓的集群运行模式 yarn ,就是提交程序到yarn 上. 要想集群运行必须指定下面三个配置.
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resoucemanager.hostname", "mini1");
        //conf.set("fs.defaultFS","com.zhouq.hdfs://mini1:9000/");

        Job job = Job.getInstance(conf);

        //指定本程序的jar 包 所在的本地路径
        job.setJarByClass(WordCountDriver.class);


        //指定本次业务的mepper 和 reduce 业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordcountReduce.class);

        //指定mapper 输出的 key  value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        //指定 最终输出的 kv  类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job 输出的文件目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean waitForCompletion = job.waitForCompletion(true);

        System.exit(waitForCompletion ? 0 : 1);
    }
}
