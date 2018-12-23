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
 * Created by zq on 2018/12/10.
 */
public class FlowCountSort {

    static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

        /**
         * 设置成员变量,避免每一次调用就创建一个对象,性能考虑.
         * <p>
         * 这里不用担心 每次设置的 bean 都是同一个对象,以致于对象的值为最后一次设置的值
         * <p>
         * 对比java 中 每次给相同的对象赋值,并加入list 列表中,最后打印的所有对象数据其实是为最后一次设置的值.
         * 因为相同的对象,数据引用地址是一致的.
         * <p>
         * 在这里我们每次需要进行序列化并输入,输出的时候值为当前设置的值.
         */

        FlowBean bean = new FlowBean();

        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 切流量
            String[] split = value.toString().split("\t");

            //取出手机号
            String phone = split[0];

            //上传流量
            long upFlow = Long.parseLong(split[split.length - 3]);
            //下载流量
            long dFlow = Long.parseLong(split[split.length - 2]);

            bean.set(upFlow, dFlow);
            v.set(value);

            context.write(bean, v);
        }
    }

    static class FlowCountSortReduce extends Reducer<FlowBean, Text, Text, FlowBean> {
        @Override
        protected void reduce(FlowBean flowBean, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            context.write(values.iterator().next(), flowBean);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //指定本程序的jar 包 所在的本地路径
        job.setJarByClass(FlowCount.class);


        //指定本次业务的mepper 和 reduce 业务类
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReduce.class);

        //指定mapper 输出的 key  value 类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);


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
