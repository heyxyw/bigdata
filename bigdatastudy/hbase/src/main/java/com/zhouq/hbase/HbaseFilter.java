package com.zhouq.hbase;

import org.apache.hadoop.hbase.filter.*;

/**
 * Created by zq on 2018/12/21.
 */
public class HbaseFilter {
    public static void main(String[] args) {


        //匹配 rowkey 为01 结尾的数据
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*01$"));

        //匹配 rowkey 包含 2017-11-12 的数据
        RowFilter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("2017-11-12"));

        //匹配 rowkey 以 123 开头的数据
        RowFilter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("123".getBytes()));

        //分页过滤器
        PageFilter pageFilter = new PageFilter(100);

        //只返回rowkey 的过滤器，value 值不返回
        KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();

        //筛选出每一行的第一个单元格
        FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();

        //

    }
}
