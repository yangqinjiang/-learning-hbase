package com.atguigu.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 测试Hbase的客户端API
 */
public class TestAPI {
    //hbase连接
    private static Connection connection = null;
    //hbase管理器
    private static Admin admin = null;
    //静态代码块
    static{
        try {
            System.out.println("初始化...");
            Configuration configuration = HBaseConfiguration.create();
            //hbase客户端首先连接zk集群,而不用连接hmaster
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //使用工厂类,连接hbase集群
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            System.out.println("初始化完成");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断hbase表是否存在
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException{
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建hbase表
     * @param tableName 表名
     * @param cfs 列族(可变参数)
     * @throws IOException
     */
    public static void  createTable(String tableName,String... cfs) throws IOException{
        if(cfs.length <= 0){
            System.out.println("请设置列族信息");
            return;
        }
        if(isTableExist(tableName)){
            System.out.println(tableName + "表已存在");
            return;
        }
        //表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //遍历列族,并增加
        for (String cf : cfs) {
            //列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
    }

    /**
     * 删除表
     * @param tableName 表名
     * @throws IOException
     */
    public  static void dropTable(String tableName) throws IOException{
        if(!isTableExist(tableName)){
            System.out.println(tableName+"表不存在");
            return;
        }
        // 首先停用(下线)表,会有一段等待时间(阻塞)
        admin.disableTable(TableName.valueOf(tableName));
        //再删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 创建命名空间
     * @param ns 命名空间名称
     */
    public static  void createNameSpace(String ns){
        //调用特定的API
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 删除命名空间
     * @param ns 命名空间名称
     */
    public static  void deleteNameSpace(String ns){
        //调用特定的API
        try {
            admin.deleteNamespace(ns);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 往hbase表添加一条数据
     * @param tableName 表名
     * @param rowKey rowkey行键
     * @param cf 列族名称
     * @param cn 列名
     * @param value 值
     * @throws IOException
     */
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException{
        // 操作表的数据时, 要创建一个table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        //addColum依次顺序为  列族, 列名, 值
        // 不需要指定时间戳
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        table.put(put);
        table.close();//用完即关闭它
    }

    /**
     * 获取hbase表的一条数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param cf 列族
     * @param cn 列名
     * @throws IOException
     */
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException{
        // 操作表的数据时, 要创建一个table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 读取一个数据
        Get get = new Get(Bytes.toBytes(rowKey));//指定rowKey
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));// 再指定 列族, 列名
        get.setMaxVersions(5);//可选, 读取多少个版本的
        //返回结果
        Result result = table.get(get);
        //打印
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    + ", CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                    +", Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();//用完即关闭它
    }

    /**
     * scan指定rowkey范围的数据
     * @param tableName 表名
     * @param startRowKey 左闭的rowkey
     * @param stopRowKey 右开的rowkey
     * @throws IOException
     */
    public static void scanTable(String tableName,String startRowKey,String stopRowKey) throws IOException{
        // 操作表的数据时, 要创建一个table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // scan startRowKey, stopRowKey
        // scan 可以查询到多行数据, 遍历其结果时, 要有两层for
        Scan scan = new Scan(Bytes.toBytes(startRowKey), Bytes.toBytes(stopRowKey));
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        "RK:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        + ", CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                        +", Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();//用完即关闭它
    }

    /**
     * 删除指定rowkey的一条数据
     * @param tableName 表名
     * @param rowKey rowkey
     * @param cf 列族
     * @param cn 列名
     * @throws IOException
     */
    public static  void deleteData(String tableName,String rowKey,String cf,String cn)throws IOException{
        // 操作表的数据时, 要创建一个table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //Delete all versions of the specified column.
        // 删除 指定列的所有版本的数据
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));//
        table.delete(delete);
        table.close();//用完即关闭它
    }
    public  static void close(){
        System.out.println("关闭Hbase连接");
        //先关闭admin
        try {
            if (admin != null) {

                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //再关闭connection
        try {
            if(connection!= null){
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //测试
    public static void main(String[] args) throws IOException {
        System.out.println("测试HbaseAPI");
        System.out.println(isTableExist("stu"));
        String non_ns_tablename = "stu-demo";
        createTable(non_ns_tablename,"info1","info2");
        System.out.println(isTableExist(non_ns_tablename));
        dropTable(non_ns_tablename);
        System.out.println(isTableExist(non_ns_tablename));

        System.out.println("测试命名空间");
        createNameSpace("namespace_demo");
        System.out.println("测试命名空间下,创建表");
        String namespace = "namespace_demo";
        String tableName = namespace+ ":stu-demo";
        createTable(tableName,"info1","info2");
        System.out.println(isTableExist(tableName));
        //添加数据
        putData(tableName,"1001","info1","name","hello");
        putData(tableName,"1001","info1","age","20");
        putData(tableName,"1002","info1","name","world");
        putData(tableName,"1002","info2","fav","pay");
        putData(tableName,"1003","info1","name","bigdata");
        //查询一条数据
        getData(tableName,"1001","info1","age");
        getData(tableName,"1002","info2","fav");

        //查询多条数据 , [1001,1003), 不包括1003的数据
        scanTable(tableName,"1001","1003");
        //清除数据
        dropTable(tableName);
        deleteNameSpace(namespace);
        close();
    }

}
    