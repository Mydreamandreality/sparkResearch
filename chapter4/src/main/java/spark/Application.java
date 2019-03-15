package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by 張燿峰
 * 测试Avg
 *
 * @author 孤
 * @date 2019/3/15
 * @Varsion 1.0
 */
public class Application {
    public static void main(String[] args) {
        rddAvg(new JavaSparkContext());
    }

    public static void rddAvg(JavaSparkContext sparkContext) {
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        RddAvg rddAvg = new RddAvg(0, 0);
        RddAvg result = javaRDD.aggregate(rddAvg, rddAvg.avgFunction2, rddAvg.rddAvgFunction2);
        System.out.println(result.avg());
    }
}
