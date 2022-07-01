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
public class Kw {
    private String keyword;
    private long score;
}
