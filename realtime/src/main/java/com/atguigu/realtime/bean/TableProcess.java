package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/6/15 10:09
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class TableProcess {
    private String sourceTable;
    private String sinkTable;
    private String sinkColumns;
    private String sinkPk;
    private String sinkExtend;
    
    private String operate_type;
}
