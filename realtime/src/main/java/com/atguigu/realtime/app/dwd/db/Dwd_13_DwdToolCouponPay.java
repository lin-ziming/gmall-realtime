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
public class Dwd_13_DwdToolCouponPay extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_13_DwdToolCouponPay().init(
            2011,
            2,
            "Dwd_13_DwdToolCouponPay",
            10
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ods_db数据
        readOdsDb(tEnv, "Dwd_13_DwdToolCouponPay");
        
        // 2. 过滤优惠券领用:  insert 数据
        Table couponUse = tEnv.sqlQuery("select " +
                                            "data['id'] id,  " +
                                            "data['coupon_id'] coupon_id,  " +
                                            "data['user_id'] user_id,  " +
                                            "date_format(data['used_time'],'yyyy-MM-dd') date_id,  " +
                                            "data['used_time'] used_time,  " +
                                            "cast(ts as string) ts " +
                                            "from ods_db " +
                                            "where `database`='gmall2022' " +
                                            "and `table`='coupon_use' " +
                                            "and `type`='update' " +
                                            "and `old`['coupon_status'] is not null " +
                                            "and `data`['coupon_status']='1403'");
        // 3. 写出到kafka中
        tEnv.executeSql("create table dwd_tool_coupon_pay (  " +
                            "id string,  " +
                            "coupon_id string,  " +
                            "user_id string,  " +
                            "date_id string,  " +
                            "used_time string,  " +
                            "ts string  " +
                            ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TOOL_COUPON_PAY));
        
        
        couponUse.executeInsert("dwd_tool_coupon_pay");
        
        
    }
}
