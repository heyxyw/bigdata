package com.zhouq;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.Random;

/**
 * Create by zhouq on 2019/2/25
 * 模拟生产消息
 */
public class TestProducer {
    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put("metadata.broker.list", "hadoop3:9092,hadoop4:9092,hadoop5:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("zookeeper.connect", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        properties.put("request.required.acks", "1");

        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String, String> producer = new Producer<>(producerConfig);

        //读取文件日志
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File("H:\\bigdatatest\\spark\\xnxt\\cmcc.log")));
            String line = null;
            while (null != (line = reader.readLine())) {

                //读取一行,发送数据到kafka
                KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>("jsonData1",new Random().nextInt(3)+ "", line);
                Thread.sleep(10);
                producer.send(keyedMessage);
                System.out.println(line);
            }
            producer.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
