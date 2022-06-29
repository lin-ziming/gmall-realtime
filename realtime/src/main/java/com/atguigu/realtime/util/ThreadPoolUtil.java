package com.atguigu.realtime.util;

import java.util.concurrent.*;

/**
 * @Author lzc
 * @Date 2022/6/29 14:16
 */
public class ThreadPoolUtil {
    public static Executor getThreadPool(){
        return new ThreadPoolExecutor(
            300,
            400,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(100)
        );
    }
}
