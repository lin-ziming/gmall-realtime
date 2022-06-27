package com.atguigu.realtime.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/17 9:22
 */
public class AtguiguUtil {
    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }
    
    public static <T>List<T> toList(Iterable<T> it) {
        ArrayList<T> result = new ArrayList<>();
        for (T t : it) {
            result.add(t);
        }
        return result;
    }
    
    // one是否大于等于two
    // "2022-06-27 01:04:48.839Z"   "2022-06-27 01:04:48.9z"
    // "2022-06-27 01:04:48.91"   "2022-06-27 01:04:48.9"
    public static boolean compareLTZ(String one, String two) {
        String oneNoZ = one.replace("Z", "");
        String twoNoZ = two.replace("Z", "");
        return oneNoZ.compareTo(twoNoZ) >= 0;
    }
    
    public static void main(String[] args) {
        System.out.println(compareLTZ("2022-06-27 01:04:48.839Z", "2022-06-28 01:04:48.822Z"));
    }
}
