package hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;
import java.sql.DriverManager;
/**
 * Created by hadoop on 2017/6/7.
 */
public class testHive {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";//jdbc驱动路径
    private static String url = "jdbc:hive2://192.168.4.103:10000/default";//hive库地址+库名
    private static String user = "hadoop";//用户名
    private static String password = "12345678";//密码
    private static String sql = "";
    private static ResultSet res;
    private static final Logger log = Logger.getLogger(testHive.class);

    public static void createTable(String tableName, Statement stmt){
        try{
            sql = "create table " + tableName + "(key int, value string) row format delimited fields terminated by '\t'";
            stmt.execute(sql);
        }catch (SQLException e){

        }

    }
    public static void dropTable(String tableName, Statement stmt){
        sql = "drop table if exists " + tableName;
        try{
            stmt.execute(sql);
        }catch (SQLException e){

        }


    }
    public static void loadData(String filePath, String tableName, Statement stmt){

        try{
            String filepath = "D:\\1.txt";
            sql = "load data inpath '"+ filepath+"' into table " + tableName;
            res = stmt.executeQuery(sql);
        }catch (SQLException e){

        }
    }
    public static ResultSet selectTable(String tableName, Statement stmt ){

        try{
            sql = "select * from " + tableName;
            res = stmt.executeQuery(sql);
            System.out.println("select result:");
        }catch (SQLException e){

        }
        return res;
    }

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            String tableName="mytest";//hive表名
            /**第一步：存在就先删除**/
//            dropTable(tableName,stmt);

            /**不存在即创建*/
//            createTable(tableName, stmt);

            //加载数据
            String filepath = "hdfs://192.168.4.102:9000/data2";
//            loadData(filepath, tableName, stmt);

            //查询插座
            res = selectTable(tableName, stmt);
            while(res.next()){
                System.out.println(res.getInt(1) + "\t" + res.getString(2));
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static Connection getConn() throws ClassNotFoundException,
            SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
}