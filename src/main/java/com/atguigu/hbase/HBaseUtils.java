package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * ClassName: HBaseUtils
 * Package: com.atguigu.hbase
 * Description:
 *
 * @Author Wang Anning
 * @Create 2024/3/26 15:19
 * @Version 1.0
 */
public class HBaseUtils {

    // 获取连接
    private static Connection connection;

    static {
        Configuration conf = new Configuration();
        // 添加Zookeeper地址，通过Zookeeper获得HBase Master地址
        conf.set("hbase.zookeeper.quorum", "localhost:2181,localhost:2182,localhost:2183");
        // 获取同步连接
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection() {
        return connection;
    }

    // 关闭连接
    public static void closeConnection(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // TODO: DDL
    // create 'namespace:table','cf...'
    public static void createTable(Connection connection, String namespace, String table, String... cfs) throws Exception {
        if (connection == null) {
            System.out.println("连接对象不能为空");
            return;
        }
        if (table == null || table.trim().isEmpty()) {
            System.out.println("表名不能为空");
            return;
        }
        if (cfs == null || cfs.length == 0) {
            System.out.println("至少指定一个列族");
            return;
        }
        // 判断表是否存在
        // 基于connection获取Admin对象
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(namespace, table);
        boolean tableExists = admin.tableExists(tn);
        if (tableExists) {
            System.out.println((namespace == null ? "default" : namespace) + ":" + table + "已经存在");
            return;
        }
        // 建表
        // 建造者获得表描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);
        // 设置列族信息
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            ColumnFamilyDescriptor columnFamilyDescriptor =
                    columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tableDescriptor);
        System.out.println((namespace == null ? "default" : namespace) + ":" + table + "创建成功");

        admin.close();
    }

    public static void main(String[] args) throws Exception {
        Connection connection = getConnection();
        // 创建表
        System.out.println("同步连接：" + connection);
        // createTable(connection,null, "t1", "f1", "f2", "f3");
        putData(connection, null, "stu", "1003", "f1", "name", "Tom");
        closeConnection(connection);
    }

    // DML新增
    public static void putData(Connection connection, String namespace, String table, String rowKey, String cf, String cl, String v) throws IOException {
        // 判空
        // 获取Table对象
        TableName tn = TableName.valueOf(namespace, table);
        Table table1 = connection.getTable(tn);
        Put put = new Put(Bytes.toBytes(rowKey));
        // 添加列
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cl), Bytes.toBytes(v));
        table1.put(put);
        table1.close();
    }

    // DML删除
    public static void deleteData(Connection connection, String namespace, String table, String rowKey, String cf, String cl) throws IOException {
        // 判空
        // 获取Table对象
        TableName tn = TableName.valueOf(namespace, table);
        Table table2 = connection.getTable(tn);

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除指定版本的数据，底层Delete
        // delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl));

        // 删除指定列所有历史数据，底层DeleteColumn
        // delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cl));

        // 删除整条数据，底层DeleteFamily
        // 什么都不用写

        // 指定列族删除
        delete.addFamily(Bytes.toBytes("cf"));


        table2.delete(delete);
        table2.close();

    }


    // DML查询
    public static void getData(Connection connection, String namespace, String table, String rowKey) throws IOException {
        // 判空
        // 获取Table对象
        TableName tn = TableName.valueOf(namespace, table);
        Table table3 = connection.getTable(tn);

        Get get = new Get(Bytes.toBytes(rowKey));

        Result result = table3.get(get);
        // 提取所有Cell
        List<Cell> cellList = result.listCells();
        // Cell[] cellArray = result.rawCells();
        for (Cell cell : cellList) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
                    + Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                    + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":"
                    + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table3.close();
    }

    // DML查询
    public static void scanData(Connection connection, String namespace, String table, String startRow, String stopRow) throws IOException {
        // 判空
        // 获取Table对象
        TableName tn = TableName.valueOf(namespace, table);
        Table table4 = connection.getTable(tn);
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table4.getScanner(scan);

        for (Result result : resultScanner) {
            List<Cell> cellList = result.listCells();
            for (Cell cell : cellList) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
                        + Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                        + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":"
                        + Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println("--------------------");
        }

        table4.close();
    }
    // DML: Scan with filter，会导致全表扫描；HBase推荐用行键
    public static void scanDataWithFilter(Connection connection, String namespace, String table) throws IOException {
        // 判空
        // 获取Table对象
        TableName tn = TableName.valueOf(namespace, table);
        Table table4 = connection.getTable(tn);
        Scan scan = new Scan();

        // 过滤
        // name = 'zhangsan'
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter
                (Bytes.toBytes("f1"), Bytes.toBytes("name"), CompareOperator.EQUAL, Bytes.toBytes("zhangsan"));
        // 对于没有这个列的数据，有两种策略，保留和跳过，默认是保留
        // 跳过
        nameFilter.setFilterIfMissing(true);
        // scan.setFilter(nameFilter);


        // age >= 30
        SingleColumnValueFilter ageFilter = new SingleColumnValueFilter
                (Bytes.toBytes("f1"), Bytes.toBytes("age"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("30"));
        ageFilter.setFilterIfMissing(true);
        // scan.setFilter(ageFilter);


        // 多个过滤条件的封装
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, nameFilter, ageFilter);
        scan.setFilter(filterList);


        ResultScanner resultScanner = table4.getScanner(scan);

        for (Result result : resultScanner) {
            List<Cell> cellList = result.listCells();
            for (Cell cell : cellList) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
                        + Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                        + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":"
                        + Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println("--------------------");
        }

        table4.close();
    }

}
