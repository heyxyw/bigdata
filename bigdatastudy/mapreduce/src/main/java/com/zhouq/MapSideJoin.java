package com.zhouq;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zq on 2018/12/13.
 */
public class MapSideJoin {

    static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


        private Map<String, String> pdMap = new HashMap<String, String>();


        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fileds = value.toString().split("\t");
            String pdName = pdMap.get(fileds[2]);
            text.set(value.toString() + "\t" + pdName);
            context.write(text, NullWritable.get());
        }


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("pd.txt")));
            String line;
            while (StringUtils.isNotBlank(line = reader.readLine())) {
                String[] fileds = line.split("\t");
                pdMap.put(fileds[0], fileds[1]);
            }

            reader.close();
        }
    }

    public static void main(String[] args) throws Exception {


        args = new String[]{"F:/hadoop-run/mr/mapsidejoin/input/", "F:/hadoop-run/mr/mapsidejoin/output19"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);


        // 将产品表文件缓存到task工作节点的工作目录中去
        job.addCacheFile(new URI("file:/F:/hadoop-run/mr/mapsidejoin/pd.txt"));

        //此任务不需要reduce
        job.setNumReduceTasks(0);

        //设置 输入 输出 目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));


        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
