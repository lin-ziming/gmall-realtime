package com.atguigu.realtime.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/7/1 10:30
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Spu {
    private String spu_name;
    private double order_amount;
}
