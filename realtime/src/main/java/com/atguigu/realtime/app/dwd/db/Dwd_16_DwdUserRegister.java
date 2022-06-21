package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSQLApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/21 10:35
 */
public class Dwd_16_DwdUserRegister extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_16_DwdUserRegister().init(
            2016,
            2,
            "Dwd_16_DwdUserRegister",
            10
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ods_db数据
        readOdsDb(tEnv, "Dwd_16_DwdUserRegister");
        
        
        // 2. 过滤出来评价表
        Table userInfo = tEnv.sqlQuery("select " +
                                           "data['id'] user_id,  " +
                                           "date_format(data['create_time'], 'yyyy-MM-dd' ) date_id,  " +
                                           "data['create_time'] create_time,  " +
                                           "cast(ts as string) ts  " +
                                           "from ods_db " +
                                           "where `database`='gmall2022' " +
                                           "and `table`='user_info' " +
                                           "and `type`='insert' ");
        
        
        tEnv.executeSql("create table `dwd_user_register`(" +
                            "`user_id` string,  " +
                            "`date_id` string,  " +
                            "`create_time` string,  " +
                            "`ts` string  " +
                            ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_USER_REGISTER));
        
        
        userInfo.executeInsert("dwd_user_register");
        
        
    }
}
