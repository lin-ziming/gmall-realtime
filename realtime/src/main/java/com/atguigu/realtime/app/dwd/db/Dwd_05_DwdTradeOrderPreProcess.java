package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/20 9:05
 */
public class Dwd_05_DwdTradeOrderPreProcess extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_05_DwdTradeOrderPreProcess().init(
            2005,
            2,
            "Dwd_05_DwdTradeOrderPreProcess",
            10
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 消费ods_db数据
        readOdsDb(tEnv);
        // 2. 读取字典表
        readBaseDic(tEnv);
        // 3. 过滤出订单详情表
        Table orderDetail = tEnv.sqlQuery("select " +
                                        " `data`['id'] id, " +
                                        " `data`['order_id'] order_id, " +
                                        " `data`['sku_id'] sku_id, " +
                                        " `data`['sku_name'] sku_name, " +
                                        " `data`['sku_num'] sku_num, " +
                                        " `data`['create_time'] create_time, " +
                                        " `data`['source_type'] source_type, " +
                                        " `data`['source_id'] source_id, " +
                                        " `data`['split_total_amount'] split_total_amount, " +
                                        " `data`['split_activity_amount'] split_activity_amount, " +
                                        " `data`['split_coupon_amount'] split_coupon_amount, " +
                                        " cast(cast(`data`['order_price'] as decimal(16,2)) * cast(`data`['sku_num'] as decimal(16,2)) as string) split_original_amount, " +
                                        " pt, " +
                                        " ts od_ts " +
                                        "from ods_db " +
                                        "where `database`='gmall2022' " +
                                        "and `table`='order_detail' " +
                                        "and `type`='insert'");
        tEnv.createTemporaryView("order_detail", orderDetail);
        
    
        // 4. 过滤出订单表
        Table orderInfo = tEnv.sqlQuery("select " +
                                        " `data`['id'] id, " +
                                        " `data`['user_id'] user_id, " +
                                        " `data`['province_id'] province_id, " +
                                        " `data`['order_status'] order_status, " +
                                        " `data`['create_time'] create_time, " +
                                        " `old`, " +
                                        " ts oi_ts " +
                                        "from ods_db " +
                                        "where `database`='gmall2022' " +
                                        "and `table`='order_info' " +
                                        "and (`type`='insert' " +
                                        " or `type`='update')");
        tEnv.createTemporaryView("order_info", orderInfo);
    
        // 5. 过滤出活动表
        
        // 6. 过滤详情优惠券表
        
        
        // 7. join
        
        // 8. 把预处理表写入到Kafka中
        
        
    }
}
