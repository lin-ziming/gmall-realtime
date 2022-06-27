package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/27 14:23
 */
public class DimUtil {
    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String dimTable, String id) {
        //select * from t where id=?
        String sql = "select * from " + dimTable + " where id=?";
        Object[] args = {id};
        List<JSONObject> result = null;
        try {
            result = JdbcUtil.<JSONObject>queryList(phoenixConn, sql, args, JSONObject.class);
        } catch (SQLException e) {
            throw new RuntimeException("sql语句有问题, 请检查sql的拼接是否正常...");
        } catch (IllegalAccessException e) {
            throw new RuntimeException("请检查你的无参构造器是否有public权限...");
        } catch (InstantiationException e) {
           // throw new RuntimeException("请给 JSONObject 提供无参构造器");
        } catch (InvocationTargetException e) {
           // throw new RuntimeException("在 JSONObject 对象找不到对应的属性....");
        }
        
        if (result.size() == 0) {
            throw new RuntimeException("没有查到对应的维度数据, 请检查表是否存在, 维度数据是否存在: 表名->" + dimTable + " id->" + id);
        }
        return result.get(0);  // 获取维度数据
    }
}
