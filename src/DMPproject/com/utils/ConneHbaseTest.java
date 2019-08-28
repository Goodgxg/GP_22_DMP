package com.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class ConneHbaseTest {
    public static void main(String[] args) throws Exception {
        Configuration conf=HBaseConfiguration.create();

        //创建连接
        Connection connection=ConnectionFactory.createConnection(conf);
        //定义表名称
        TableName tableName=TableName.valueOf("myns1:stu_info");
        //获取表的实例
        Table table=connection.getTable(tableName);
        //定义rowkey
        Get rowKey=new Get(Bytes.toBytes("1001"));
        //定义结果集
        Result result=table.get(rowKey);
        //定义单元格数组,并从result结果集中拿来转换单元格
        Cell[] cells=result.rawCells();
        //遍历单元格
        for(Cell cell:cells){
            //取得rowkey内容
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));

        }
    }
}
