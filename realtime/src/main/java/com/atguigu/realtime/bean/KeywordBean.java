package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/6/24 11:13
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeywordBean {
    private String stt;
    private String edt;
    private String source;
    private String keyword;
    private Long keywordCount;
    private Long ts;
}
