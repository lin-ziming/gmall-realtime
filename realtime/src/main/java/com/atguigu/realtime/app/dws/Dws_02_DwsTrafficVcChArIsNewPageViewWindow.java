package com.atguigu.realtime.app.dws;

/**
 * @Author lzc
 * @Date 2022/6/24 14:44
 */
public class Dws_02_DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) {
    
    }
}
/*

不同的维度:


五个指标:
会话数
    页面日志
浏览总时长
    页面日志

页面浏览数  pv
    页面日志
独立访客数  uv
    来源uv详情表
跳出会话数  uj
    来源uj详情表
------------------------

3个流:
union

--------

小米..     1   0   0
小米..     0   1   0
小米..     1   0   0


聚合:
按照维度开窗聚合
  keyBy().window().reduce()...
-----------------------------------
维度    窗口    pv  uv  uj
小米..  0-5     10   3   1
华为..  0-5     20   13   3
 */