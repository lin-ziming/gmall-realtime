package com.atguigu.realtime.app.dwd.db;

/**
 * @Author lzc
 * @Date 2022/6/18 10:51
 */
public class Dwd_04_DwdTradeCartAdd {
}

/*
数据源:
    ods_db

过滤:
    出来加购物车
    
    新加入和修改(个数增加导致)的
    
        苹果手机   1
        华为手机   1
        苹果手机   4    从1变化为了4
            新记录应该是 4 or 3√
            
sql语句:

1. 建立动态表与 kafka的topic进行关联 :ods_db

2. 过滤出购物车数据

3. 维度退化: base_dic中的维度信息退化到事实表中
    join
     表与表的join
        事实表和维度表的join
            lookup join

3. 把结果写入到kafka中
        



*/
