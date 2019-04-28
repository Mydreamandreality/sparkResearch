package datasource;

import scala.Int;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 張燿峰
 *
 * @author 孤
 * @date 2019/4/24
 * @Varsion 1.0
 */
public class Test {
    /*这是程序的入口*/
    public static void main(String[] args) {
        //Java的代码必须以 ; 号结尾
        //Java的基础数据类型分为 byte,short,int,long,double,float,char,String 区别在于字节数不同
        // [String属于特殊的基础数据类型] 除基础类型还有引用类型,这个暂时无需了解

        //Java运行在JVM之上的,所以Java代码运行会编译成.class文件,运行在虚拟机上

        //需求:计算Ip地址为160段的总数

        //1.定义集合,存放IP地址,集合可动态增加删除,开发中这个最常用
        List<String> ips = new ArrayList<>();   //初始化集合,<String>代表集合中的元素数据类型,还有LinkList,Set等其它集合,区别在于线程安全与否,暂时无需关注
        ips.add("192.168.160.1");
        ips.add("192.168.160.2");
        ips.add("192.168.000.2");
        ips.add("192.168.000.2");       //add添加元素
        //ips.add(1);                   //这行代码是错误的,因为我们使用的是泛型集合,泛型的类型定义为String,不允许add Int类型的数据

        Integer foreachCount = 0;
        /*第一种循环方式 foreach*/
        for (String result : ips) {
            String subResult = result.substring(8, result.lastIndexOf("."));     //截取Ip地址字符串,获取网段
            if ("160".equals(subResult)) {           //开发中:160必须在equals条件的左侧,防止空指针异常  :空指针异常时Java程序中经常遇到的错误
                foreachCount++;                            //结果自增+1
            }
        }
        System.out.println("foreach循环输出结果:" + foreachCount);                  //输出160段的Ip地址总数


        Integer forCount = 0;
        /*第二种循环方式 foreach*/
        for (int i = 0; i < ips.size(); i++) {
            String subResult = ips.get(i).substring(8, ips.get(i).lastIndexOf("."));     //截取Ip地址字符串,获取网段
            if ("160".equals(subResult)) {           //开发中:160必须在equals条件的左侧,防止空指针异常  :空指针异常时Java程序中经常遇到的错误
                forCount++;                            //结果自增+1
            }
        }
        System.out.println("for循环输出结果:" + forCount);
    }
}
