/**
 * @Author lzc
 * @Date 2022/6/15 11:23
 */
public class Test1 {
    public static void main(String[] args) {
        String s = "id,activity_id,sku_id,create_time";
        
        System.out.println(s.replaceAll("([^,]+)", "$1 varchar"));
        System.out.println(s.replaceAll(",", " varchar,") + " varchar");
    }
}
