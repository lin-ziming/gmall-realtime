package com.atguigu.realtime.util;

import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/15 10:39
 */
public class JdbcUtil {
    public static Connection getPhoenixConnection() {
        
        String driver = Constant.PHOENIX_DRIVER;
        String url = Constant.PHOENIX_URL;
        Connection conn = null;
        try {
            conn = getJdbcConnect(driver, url, null, null);
        } catch (ClassNotFoundException e) {
            // 驱动没有找到
            throw new RuntimeException("phoenix的驱动类没有找到, 请确认是导入了phoenix相关的依赖...");
        } catch (SQLException e) {
            throw new RuntimeException("无法连接到phoenix, 请检查zookeeper地址是否正确和zookeeper是否正常开启...");
        }
        return conn;
    }
    
    private static Connection getJdbcConnect(String driver,
                                             String url,
                                             String user,
                                             String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);  // 机制驱动
        return DriverManager.getConnection(url, user, password);
    }
    
    public static void main(
        String[] args) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        //        List<JSONObject> list = queryList(getPhoenixConnection(), "select * from dim_sku_info where id=?", new Object[]{"1"}, JSONObject.class);
        List<TableProcess> list = queryList(getJdbcConnect("com.mysql.cj.jdbc.Driver", "jdbc:mysql://hadoop162:3306/gmall_config?useSSL=false", "root", "aaaaaa"),
                                            "select * from table_process",
                                            null,
                                            TableProcess.class);
        for (TableProcess object : list) {
            
            System.out.println(object);
        }
    }
    
    
    // 实现这个方法: 使用传来的连接对象, 执行sql'语句, 把查询到的结果封装到List集合中
    public static <T> List<T> queryList(Connection conn,
                                        String sql,
                                        Object[] args,
                                        Class<T> tClass,
                                        Boolean ... underlineToCaseCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        ArrayList<T> result = new ArrayList<>();
        boolean isToCamel = false;
        if (underlineToCaseCamel.length > 0) {
            isToCamel = underlineToCaseCamel[0];
        }
      
        // 1. 获取预处理语句s
        PreparedStatement ps = conn.prepareStatement(sql);
        // 2. 给占位符赋值
        for (int i = 0; args != null && i < args.length; i++) {
            Object arg = args[i];
            ps.setObject(i + 1, arg);
        }
        // 3 执行查询
        ResultSet resultSet = ps.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();  // 获取表的元数据
        int columnCount = metaData.getColumnCount();// 获取查询到的列数
        // 4. 把查询到的结果封装到 List集合中返回
        while (resultSet.next()) {  // 表示有一行数据, 并把指针移动到了这行数据上
            // 读取到一行数据
            // 把这行数据所有的列全部读出来, 封装到一个T类型的对象中
            T t = tClass.newInstance();
            
            // 遍历这行的每一列
            for (int i = 1; i <= columnCount; i++) {
                // 读取列名
                String columnName = metaData.getColumnLabel(i);
                
                
                if (isToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                Object columnValue = resultSet.getObject(i);
                // 把值赋值T对象中的同名属性(pojo), 或者坐车key-value进行赋值
                BeanUtils.setProperty(t, columnName, columnValue);
            }
            
            result.add(t);
        }
        
        
        return result;
    }
}
