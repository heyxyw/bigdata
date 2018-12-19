package com.zhouq.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by zq on 2018/12/19.
 */
public class HbaseDDL {

    Connection connection;

    @Before
    public void getConn() throws Exception{
        //构建一个连接对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","mini1:2181,mini2:2181,mini3:2181");
        connection = ConnectionFactory.createConnection(conf);
    }

    /**
     * 新建表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {


        //从连接器中构造一个DDL 操作器
        Admin admin = connection.getAdmin();

        //构造一个表定义描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));

        //构造一个列簇定义描述器
        HColumnDescriptor hColumnDescriptor_1 = new HColumnDescriptor("base_info");

        //设置该列簇的最大版本号，默认是1
        hColumnDescriptor_1.setMaxVersions(3);

        HColumnDescriptor hColumnDescriptor_2 = new HColumnDescriptor("ex_info");

        // 将列簇定义信息加入表定义对象中
        hTableDescriptor.addFamily(hColumnDescriptor_1)
                .addFamily(hColumnDescriptor_2);

        // 用ddl 操作对象 来建表
        admin.createTable(hTableDescriptor);

        // 关闭资源
        admin.close();
        connection.close();
    }


    /**
     * 删除表
     * @throws IOException
     */
    @Test
    public void deleteTable() throws IOException {
        Admin admin = connection.getAdmin();

        // 停用表
        admin.disableTable(TableName.valueOf("user_info"));
        // 删除表
        admin.deleteTable(TableName.valueOf("user_info"));

        admin.close();
        connection.close();
    }

    /**
     * 修改表定义
     */
    @Test
    public void testAlterTable() throws IOException {
        Admin admin = connection.getAdmin();

        //取出旧的表定义信息
        HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));

        // 重新构造一个列簇定义
        HColumnDescriptor other_info = new HColumnDescriptor("other_info");
        //设置列簇的布隆过滤器
        other_info.setBloomFilterType(BloomType.ROW);


        //删除表定义
        hTableDescriptor.removeFamily("other_info1".getBytes());

        //将新列簇放到表定义对象中
        hTableDescriptor.addFamily(other_info);

        admin.modifyTable(TableName.valueOf("user_info"),hTableDescriptor);

        admin.close();
        connection.close();

    }

}
