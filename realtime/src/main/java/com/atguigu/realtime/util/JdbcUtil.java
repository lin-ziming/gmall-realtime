package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
    
    private static Connection getJdbcConnect(String driver, String url, String user,
                                             String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);  // 机制驱动
        return DriverManager.getConnection(url, user, password);
    }
    
    
}
