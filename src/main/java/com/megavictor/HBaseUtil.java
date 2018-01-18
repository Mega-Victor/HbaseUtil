package com.megavictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HBaseUtil {

    public static Configuration configuration;
    public static Connection connection;

    public static void main(String[] args) throws IOException, ParseException {
        init();

        // 传入时间参数 用来匹配数据文件
        String yesterday = args[0];
        // 传入配置文件名参数
        String confName = args[1];
        // 读取配置文件
        Properties prop = new Properties();
        InputStream inputStream = HBaseUtil.class.getResourceAsStream("/" + confName);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            prop.load(bufferedReader);
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
            try {
                bufferedReader.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        // colNum为数据的列数
        int colNum = Integer.parseInt(prop.get("colNum").toString());
        //flag 作为标识
        int flag = Integer.parseInt(prop.get("flag").toString());
        String fileName = prop.get("fileName") + yesterday;
        String tableName = prop.getProperty("tableName");
        String rowKey = prop.getProperty("rowKey");
        String colFamily = prop.getProperty("colFamily");
        System.out.println(fileName);
        System.out.println(tableName);
        System.out.println(colNum);

        // 读取数据文件
        List<List<Object>> lists = readFileByLines(fileName, colNum);

        for (int rowLength = 0; rowLength < lists.size(); rowLength++) {
            List<Object> row = lists.get(rowLength);
            addRow(tableName, rowKey, colFamily, "col", "val");
        }

        close();
    }


    /**
     * 读取文件 文件命名格式: 属性_日期 app_active_20180108
     *
     * @param fileName 文件名
     * @param colNum   文件数据列数
     * @return 返回每行数据组成的list
     */
    public static List<List<Object>> readFileByLines(String fileName, int colNum) {
        File file = new File(fileName);
        BufferedReader reader = null;
        List<List<Object>> list = new ArrayList<List<Object>>();

        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {

                String[] aa = tempString.split("\\s+");
                List<Object> l = new ArrayList<Object>();
                for (int i = 0; i < colNum; i++) {
                    // 判断数据是否为null 为null则转为0
                    if (i == colNum - 1 && aa[colNum - 1].equals("NULL"))
                        l.add("0");
                    else
                        l.add(aa[i]);
                }

                list.add(l);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return list;
    }

    //初始化链接
    public static void init() {
        configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", ""192.168.1.78,192.168.1.80,192.168.1.84"");
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("zookeeper.znode.parent", "/hbase");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    public static void close() {
        try {
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 创建表格
    public static void createTable(String tableName, String[] colFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(connection);

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (int i = 0; i < colFamily.length; i++) {
            tableDescriptor.addFamily(new HColumnDescriptor(colFamily[i]));
        }

        admin.createTable(tableDescriptor);
    }

    //插入数据
    public static void addRow(String tableName, String rowkey, String colFamily, String col, String val) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        //批量插入
       /* List<Put> putList = new ArrayList<Put>();
        puts.add(put);
        table.put(putList);*/
        table.close();
    }

    //删除数据

    public static void deleRow(String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        //批量删除
       /* List<Delete> deleteList = new ArrayList<Delete>();
        deleteList.add(delete);
        table.delete(deleteList);*/
        table.close();
    }

    public static void deleRow(String tableName, String rowkey, String colFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        //删除指定列族
        delete.addFamily(Bytes.toBytes(colFamily));
        table.delete(delete);
        table.close();
    }

    public static void deleRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        //删除指定列
        delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        table.delete(delete);
        table.close();
    }

    //根据rowkey查找数据
    public static void getData(String tableName, String rowkey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        //获取指定列族数据
        //get.addFamily(Bytes.toBytes(colFamily));
        //获取指定列数据
        //get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);

        showCell(result);
        table.close();
    }

    //格式化输出
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.print("RowKey:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.print("Timetamp:" + cell.getTimestamp() + " ");
            System.out.print("Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.print("Qualifier:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.print("value:" + new String(CellUtil.cloneValue(cell)) + " ");
            System.out.println();
        }
    }

    //批量查找数据
    public static void scanData(String tableName, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //scan.setStartRow(Bytes.toBytes(startRow));
        //scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            showCell(result);
        }
        table.close();
    }


}
