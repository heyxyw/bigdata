package com.zhouq.mrjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zq on 2018/12/13.
 */
public class Rjoin {


    static class RjoinMapper extends Mapper<LongWritable, Text, Text, RjoinBean> {

        RjoinBean rjoinBean = new RjoinBean();

        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            String[] split = line.split("\t");

            // 产品ID
            String p_id = "";

            //看文件是 订单表 还是 产品表
            if (fileName.contains("order")) {
                //订单表
                //1001	20150710	P0001	2
                p_id = split[2];
                rjoinBean.set(Integer.parseInt(split[0]), split[1], split[2], Integer.parseInt(split[3]), "", 0, "1");

            } else {
                //产品表
                //P0001	小米5	1001	2
                p_id = split[0];
                rjoinBean.set(0, "", p_id, 0, split[1], Float.parseFloat(split[2]), "0");
            }

            text.set(p_id);
            context.write(text, rjoinBean);

        }
    }

    static class RjoinReduce extends Reducer<Text,RjoinBean,RjoinBean,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<RjoinBean> values, Context context) throws IOException, InterruptedException {

            //拿到产品 bean
            RjoinBean pd = new RjoinBean();

            //订单列表
            List<RjoinBean> orders = new ArrayList<RjoinBean>();

            for (RjoinBean value : values) {
                if ("0".equals(value.getFlag())){
                    try {
                        BeanUtils.copyProperties(pd,value);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }else{
                    RjoinBean rjoinBean = new RjoinBean();
                    try {
                        BeanUtils.copyProperties(rjoinBean,value);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    orders.add(rjoinBean);
                }
            }

            for (RjoinBean order : orders) {
                order.setProduce_name(pd.getProduce_name());
                order.setPrice(pd.getPrice());
                context.write(order,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {

        args = new String[]{"F:/hadoop-run/mr/rjoin/input/","F:/hadoop-run/mr/rjoin/output6/"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指定本程序jar 所在地
//        job.setJarByClass(Rjoin.class);

        // 指定本程序的 mapper 和 reduce  class
        job.setMapperClass(RjoinMapper.class);
        job.setReducerClass(RjoinReduce.class);

        //指定map阶段输出 的key  value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RjoinBean.class);

        // 指定reduce 阶段的 key value 类型
        job.setOutputKeyClass(RjoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定 输入路径，输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        TextOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0: -1);
    }
}
