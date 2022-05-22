package com.imooc.spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase操作工具类：Java工具类建议采用单例模式封装
 */
public class HBaseUtils {
    HBaseAdmin hadmin = null;
    Configuration configuration = null;

    // 单例模式1 创建私有变量
    private static HBaseUtils instance = null;

    // 单例模式2 创建私有构造方法
    private HBaseUtils() {
        configuration = new Configuration();
        // 这里的值和 hbase-site.xml 相关
        configuration.set("hbase.zookeeper.quorum", "hadoop000:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop000:8020/hbase");
        try {
            hadmin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 单例模式3 初始化私有变量(synchronized是为了线程安全)
    public static synchronized HBaseUtils getInstance() {
        if (instance == null) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    // 根据表名, 获取 HTable 对象
    public HTable getTable(String tableName) {
        HTable hTable = null;
        try {
            hTable = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hTable;
    }

    /**
     * 添加一条记录到HBase表
     * @param tableName HBase表名
     * @param rowKey    HBase表的rowkey
     * @param cf        HBase表的columnfamily
     * @param column    HBase表的列
     * @param value     HBase表的值
     */
    public void putData(String tableName, String rowKey, String cf, String column, Long value) {
        // 根据表名, 获取 HTable 对象
        HTable hTable = getTable(tableName);
        // 根据rowKey, 获取 Put 对象
        Put hPut = new Put(Bytes.toBytes(rowKey));
        // 给 Put 对象绑定要添加的数据
        hPut.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        // 提交 Put 对象
        try {
            hTable.put(hPut);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0");

        // 获取表明
        // HTable hTable = HBaseUtils.getInstance().getTable("imooc_course_clickcount");
        // System.out.println(hTable.getName().toString());

        // 添加数据
        String tableName = "imooc_course_clickcount";
        String rowKey = "20171111_1";
        String cf = "info";
        String column = "c1";
        Long value = Long.valueOf(33);
        HBaseUtils.getInstance().putData(tableName, rowKey, cf, column, value);
    }
}
