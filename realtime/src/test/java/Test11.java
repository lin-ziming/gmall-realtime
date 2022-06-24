import com.atguigu.realtime.bean.KeywordBean;

import java.lang.reflect.Field;

/**
 * @Author lzc
 * @Date 2022/6/24 13:52
 */
public class Test11 {
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        // 几种方式获取class
        // 1. Class.forName("字符串类名")  2. 类名.class  3. 对象.getClass
    
        KeywordBean bean = new KeywordBean();
        bean.setStt("abc");
    
        Class<KeywordBean> tClass = KeywordBean.class;
       
        
    
        //        Field[] fields = tClass.getFields();  // 获取类中所有的 public 属性
        /*Field[] fields = tClass.getDeclaredFields();
        for (Field field : fields) {
            System.out.println(field.getName());
        }*/
    
        Field stt = tClass.getDeclaredField("stt");
        stt.setAccessible(true);  // 设置允许访问私有属性
       //  bean.stt
//        Object o = stt.get(bean);
//        System.out.println(o);
        
        stt.set(bean, "hello");
    
        System.out.println(bean.getStt());
    
    
    }
}
