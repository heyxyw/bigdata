package com.zhouq.applog.log.mr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * app log日志清洗 mr
 */
public class AppLogDataClear {

    static class AppLogDataClearMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = null;
        NullWritable v;
        SimpleDateFormat sdf;

        MultipleOutputs mos = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            k = new Text();
            v = NullWritable.get();
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //多路复用器

            mos = new MultipleOutputs(context);

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //解析数据

            JSONObject parse = JSON.parseObject(value.toString());

            JSONObject header = parse.getJSONObject("header");

            // 过滤必填字段数据为空的
            // 采集SDK版本号
            if (checkFiledsIsBlack(header, "sdk_ver")) {
                return;
            }

            if (checkFiledsIsBlack(header, "time_zone")) {
                return;
            }

            if (checkFiledsIsBlack(header, "commit_id")) {
                return;
            }



            if (null == header.getString("commit_time") || "".equals(header.getString("commit_time").trim())) {
                return;
            }else{
                // 练习时追加的逻辑，替换掉原始数据中的时间戳
                String commit_time = header.getString("commit_time");
                String format = sdf.format(new Date(Long.parseLong(commit_time)+38*24*60*60*1000L));
                header.put("commit_time", format);
            }

            if (checkFiledsIsBlack(header, "pid")) {
                return;
            }
            if (checkFiledsIsBlack(header, "app_token")) {
                return;
            }

            if (checkFiledsIsBlack(header, "app_id")) {
                return;
            }
            if (checkFiledsIsBlack(header, "device_id" )|| header.getString("device_id").length()<17) {
                return;
            }
            if (checkFiledsIsBlack(header, "device_id_type")) {
                return;
            }
            if (checkFiledsIsBlack(header, "release_channel")) {
                return;
            }
            if (checkFiledsIsBlack(header, "app_ver_name")) {
                return;
            }
            if (checkFiledsIsBlack(header, "app_ver_code")) {
                return;
            }
            if (checkFiledsIsBlack(header, "os_name")) {
                return;
            }
            if (checkFiledsIsBlack(header, "os_ver")) {
                return;
            }
            if (checkFiledsIsBlack(header, "language")) {
                return;
            }
            if (checkFiledsIsBlack(header, "country")) {
                return;
            }
            if (checkFiledsIsBlack(header, "manufacture")) {
                return;
            }
            if (checkFiledsIsBlack(header, "device_model")) {
                return;
            }
            if (checkFiledsIsBlack(header, "resolution")) {
                return;
            }
            if (checkFiledsIsBlack(header, "net_type")) {
                return;
            }
            if (checkFiledsIsBlack(header, "language")) {
                return;
            }

            // 生成user_id
            // 如果是安卓 设备，使用android_id 作为用户id ,没有android_id 使用 device_id

            String user_id = "";
            if ("android".equals(header.getString("os_name").trim())) {
                user_id = StringUtils.isNotBlank(header.getString("android_id")) ? header.getString("android_id") : header.getString("device_id");
            } else {
                user_id = header.getString("device_id");
            }

            header.put("user_id", user_id);


            // 输出
            k.set(toString(header));

            if ("android".equals(header.getString("os_name"))) {
                mos.write(k, v, "android/android");
            } else {
                mos.write(k, v, "ios/ios");
            }


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

        /**
         * 自定义输出字符串 按  分割
         *
         * @param jsonObj
         * @return
         */
        public static String toString(JSONObject jsonObj) {
            StringBuilder sb = new StringBuilder();
            sb.append(jsonObj.get("sdk_ver")).append("\001").append(jsonObj.get("time_zone")).append("\001")
                    .append(jsonObj.get("commit_id")).append("\001").append(jsonObj.get("commit_time")).append("\001")
                    .append(jsonObj.get("pid")).append("\001").append(jsonObj.get("app_token")).append("\001")
                    .append(jsonObj.get("app_id")).append("\001").append(jsonObj.get("device_id")).append("\001")
                    .append(jsonObj.get("device_id_type")).append("\001").append(jsonObj.get("release_channel"))
                    .append("\001").append(jsonObj.get("app_ver_name")).append("\001").append(jsonObj.get("app_ver_code"))
                    .append("\001").append(jsonObj.get("os_name")).append("\001").append(jsonObj.get("os_ver"))
                    .append("\001").append(jsonObj.get("language")).append("\001").append(jsonObj.get("country"))
                    .append("\001").append(jsonObj.get("manufacture")).append("\001").append(jsonObj.get("device_model"))
                    .append("\001").append(jsonObj.get("resolution")).append("\001").append(jsonObj.get("net_type"))
                    .append("\001").append(jsonObj.get("account")).append("\001").append(jsonObj.get("app_device_id"))
                    .append("\001").append(jsonObj.get("mac")).append("\001").append(jsonObj.get("android_id"))
                    .append("\001").append(jsonObj.get("imei")).append("\001").append(jsonObj.get("cid_sn")).append("\001")
                    .append(jsonObj.get("build_num")).append("\001").append(jsonObj.get("mobile_data_type")).append("\001")
                    .append(jsonObj.get("promotion_channel")).append("\001").append(jsonObj.get("carrier")).append("\001")
                    .append(jsonObj.get("city")).append("\001").append(jsonObj.get("user_id"));

            return sb.toString();
        }

        public boolean checkFiledsIsBlack(JSONObject headerObj, String filed) {

            return StringUtils.isBlank(headerObj.getString(filed));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(AppLogDataClear.class);

        job.setMapperClass(AppLogDataClearMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
