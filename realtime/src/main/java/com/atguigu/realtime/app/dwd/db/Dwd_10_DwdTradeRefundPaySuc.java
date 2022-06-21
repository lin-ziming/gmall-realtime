package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSQLApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/21 10:04
 */
public class Dwd_10_DwdTradeRefundPaySuc extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_10_DwdTradeRefundPaySuc().init(
            2010,
            2,
            "Dwd_10_DwdTradeRefundPaySuc",
            10
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 读取ods_db
        readOdsDb(tEnv, "Dwd_10_DwdTradeRefundPaySuc");
        // 2. 读取字典表
        readBaseDic(tEnv);
        // 3. 过滤退款表
        Table refundPayment = tEnv.sqlQuery("select " +
                                                "data['id'] id,  " +
                                                "data['order_id'] order_id,  " +
                                                "data['sku_id'] sku_id,  " +
                                                "data['payment_type'] payment_type,  " +
                                                "data['callback_time'] callback_time,  " +
                                                "data['total_amount'] total_amount,  " +
                                                "pt,  " +
                                                "ts  " +
                                                "from ods_db " +
                                                "where `database`='gmall2022' " +
                                                "and `table`='refund_payment' "
                                       /* + "and `type`='update' " +   // 由于模拟数据的问题, 需要注释
                                        "and `old`['refund_status'] is not null " +
                                        "and `data`['refund_status']='0705'"*/
        );
        tEnv.createTemporaryView("refund_payment", refundPayment);
        // 4. 过滤订单表
        Table orderInfo = tEnv.sqlQuery("select " +
                                            "data['id'] id,  " +
                                            "data['user_id'] user_id,  " +
                                            "data['province_id'] province_id,  " +
                                            "`old`  " +
                                            "from ods_db " +
                                            "where `database`='gmall2022' " +
                                            "and `table`='order_info' " +
                                            "and `type`='update' " +
                                            "and `old`['order_status'] is not null " +
                                            "and `data`['order_status']='1006'");
        tEnv.createTemporaryView("order_info", orderInfo);
        // 5. 过滤退单表
        Table orderRefundInfo = tEnv.sqlQuery("select " +
                                                  "data['id'] id, " +
                                                  "data['user_id'] user_id, " +
                                                  "data['order_id'] order_id, " +
                                                  "data['sku_id'] sku_id, " +
                                                  "data['refund_type'] refund_type, " +
                                                  "data['refund_num'] refund_num, " +
                                                  "data['refund_amount'] refund_amount, " +
                                                  "data['refund_reason_type'] refund_reason_type, " +
                                                  "data['refund_reason_txt'] refund_reason_txt, " +
                                                  "data['create_time'] create_time, " +
                                                  "pt, " +
                                                  "ts " +
                                                  "from ods_db " +
                                                  "where `database`='gmall2022' " +
                                                  "and `table`='order_refund_info' "
                                 /*                 + "and `type`='update' " +
                                                  "and `old`['refund_status'] is not null " +
                                                  "and `data`['refund_status']='0705'"*/
        );
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        
        // 6. 四张表join
        Table result = tEnv.sqlQuery("select " +
                                        "rp.id,  " +
                                        "oi.user_id,  " +
                                        "rp.order_id,  " +
                                        "rp.sku_id,  " +
                                        "oi.province_id,  " +
                                        "rp.payment_type,  " +
                                        "dic.dic_name payment_type_name,  " +
                                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id,  " +
                                        "rp.callback_time,  " +
                                        "ri.refund_num,  " +
                                        "rp.total_amount,  " +
                                        "cast (rp.ts as string),  " +
                                        "current_row_timestamp() row_op_ts  " +
                                        "from refund_payment rp " +
                                        "join order_info oi on rp.order_id=oi.id " +
                                        "join order_refund_info ri on rp.sku_id=ri.sku_id " +  // 连接的时候注意不要用订单id, 应该用sku_id
                                        "join base_dic for system_time as of rp.pt as dic on rp.payment_type=dic.dic_code ");
        
        // 7. 写出到kafka中
        tEnv.executeSql("create table dwd_trade_refund_pay_suc(  " +
                                "id string,  " +
                                "user_id string,  " +
                                "order_id string,  " +
                                "sku_id string,  " +
                                "province_id string,  " +
                                "payment_type_code string,  " +
                                "payment_type_name string,  " +
                                "date_id string,  " +
                                "callback_time string,  " +
                                "refund_num string,  " +
                                "refund_amount string,  " +
                                "ts string,  " +
                                "row_op_ts timestamp_ltz(3)  " +
                                ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC));
    
        result.executeInsert("dwd_trade_refund_pay_suc");
    
    
    }
    
}
