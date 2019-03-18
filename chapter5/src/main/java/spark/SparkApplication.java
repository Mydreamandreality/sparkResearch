package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Created by 張燿峰
 * 案例测试
 *
 * @author 孤
 * @date 2019/3/18
 * @Varsion 1.0
 */
public class SparkApplication {

    public static void run(JavaSparkContext sparkContext) {
        JavaRDD<Integer> javaRDD = sparkContext.parallelize
                (Arrays.asList(1, 2, 323, 434123, 2, 123213, 24325, 123213, 99, 132));

        List<Integer> toJavaRDD = javaRDD.top(3, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                //这里实现排序的具体算法
                return 0;
            }
        });

        javaRDD.takeOrdered(2, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });
    }
}
