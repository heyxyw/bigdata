package com.zhouq.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zq on 2018/12/19.
 */
public class HbaseDemoDML {

    Connection connection;

    @Before
    public void getConn() throws Exception {
        //构建一个连接对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "mini1:2181,mini2:2181,mini3:2181");
        connection = ConnectionFactory.createConnection(conf);
    }

    /**
     * 添加
     */
    @Test
    public void testPut() throws IOException {

        Table userInfo_table = connection.getTable(TableName.valueOf("user_info"));

        Put put = new Put(Bytes.toBytes("0001"));
        //封装数据
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("zhouq"));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes(18));
        put.addColumn(Bytes.toBytes("ex_info"),Bytes.toBytes("addr"),Bytes.toBytes("四川成都市。。。。。"));

        Put put1 = new Put(Bytes.toBytes("0002"));
        put1.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("xyw"));
        put1.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes(17));
        put1.addColumn(Bytes.toBytes("ex_info"),Bytes.toBytes("addr"),Bytes.toBytes("四川成都市。。。。。"));


        //插入
        userInfo_table.put(put);
        userInfo_table.put(put1);

        userInfo_table.close();
        connection.close();
    }


    @Test
    public void testGet() throws IOException {

        Table user_info = connection.getTable(TableName.valueOf("user_info"));
//
//        Get get = new Get();
//
//        user_info.get(get);

    }


    @Test
    public void testDelete() throws Exception{
        Table user_info = connection.getTable(TableName.valueOf("user_info"));

        Delete delete = new Delete(Bytes.toBytes("0001"));

        Delete delete1 = new Delete(Bytes.toBytes("0002"));
        delete1.addColumn(Bytes.toBytes("ex_info"),Bytes.toBytes("addr"));

        List<Delete> deletes = Arrays.asList(delete, delete1);

        user_info.delete(deletes);
        user_info.close();
        connection.close();
    }

}
