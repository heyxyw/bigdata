package com.zhouq.flowcount;

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
 *
 * Created by zq on 2018/12/10.
 */
public class FlowCount {


    static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 切流量
            String[] split = value.toString().split("\t");

            //取出手机号
            String phone = split[1];

            //上传流量
            long upFlow = Long.parseLong(split[split.length - 3]);
            //下载流量
            long dFlow = Long.parseLong(split[split.length - 2]);

            context.write(new Text(phone), new FlowBean(upFlow, dFlow));
        }
    }


    static class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

        // <18111,FlowBean> , <18111,FlowBean2> ........
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            long sum_upFlow = 0;
            long sum_dFlow = 0;

            for (FlowBean flowBean : values) {
                sum_dFlow += flowBean.getdFlow();
                sum_upFlow += flowBean.getUpFlow();
            }
            context.write(key, new FlowBean(sum_upFlow, sum_dFlow));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //指定本程序的jar 包 所在的本地路径
        job.setJarByClass(FlowCount.class);


        //指定本次业务的mepper 和 reduce 业务类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        //指定mapper 输出的 key  value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);


        //指定 最终输出的 kv  类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job 输出的文件目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean waitForCompletion = job.waitForCompletion(true);

        System.exit(waitForCompletion ? 0 : 1);
    }
}
