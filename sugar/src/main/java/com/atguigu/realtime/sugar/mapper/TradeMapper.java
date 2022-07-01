package com.atguigu.realtime.sugar.mapper;

import com.atguigu.realtime.sugar.bean.Spu;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TradeMapper {

    @Select("SELECT sum(order_amount) " +
        "FROM dws_trade_sku_order_window " +
        "WHERE toYYYYMMDD(stt) = #{date}")
    Double gmv(int date);
    
    @Select("SELECT\n" +
        "    spu_name,\n" +
        "    sum(order_amount) AS order_amount\n" +
        "FROM dws_trade_sku_order_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY spu_name")
    List<Spu> gmvBySpu(int date);
    
}
