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
public class Dwd_14_DwdInteractionFavorAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_14_DwdInteractionFavorAdd().init(
            2011,
            2,
            "Dwd_14_DwdInteractionFavorAdd",
            10
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ods_db数据
        readOdsDb(tEnv, "Dwd_14_DwdInteractionFavorAdd");
        
        Table favorInfo = tEnv.sqlQuery("select " +
                                            "data['id'] id,  " +
                                            "data['user_id'] user_id,  " +
                                            "data['sku_id'] sku_id,  " +
                                            "date_format(data['create_time'],'yyyy-MM-dd') date_id,  " +
                                            "data['create_time'] create_time,  " +
                                            "cast(ts as string) ts " +
                                            "from ods_db " +
                                            "where `database`='gmall2022' " +
                                            "and `table`='favor_info' " +
                                            "and `type`='insert' " );
    
    
        tEnv.executeSql("create table dwd_interaction_favor_add (  " +
                                "id string,  " +
                                "user_id string,  " +
                                "sku_id string,  " +
                                "date_id string,  " +
                                "create_time string,  " +
                                "ts string  " +
                                ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_INTERACTION_FAVOR_ADD));
    
        favorInfo.executeInsert("dwd_interaction_favor_add");
    
    
    }
}
