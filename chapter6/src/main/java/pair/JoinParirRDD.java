package pair;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by 張燿峰
 * 第七章案例代码
 * @author 孤
 * @date 2019/3/21
 * @Varsion 1.0
 */
public class JoinParirRDD {

    public static void run(JavaSparkContext sparkContext){
        JavaRDD<String> rdd = sparkContext.parallelize(Arrays.asList("test", "java", "python"));
        JavaRDD<String> otherRDD = sparkContext.parallelize(Arrays.asList("golang", "php", "hadoop"));

        PairFunction<String, String, String> pairFunction = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                return new Tuple2<>(s.split(" ")[0], s);
            }
        };
        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(pairFunction);
        JavaPairRDD<String, String> pairRDDOther = otherRDD.mapToPair(pairFunction);

        pairRDD.sortByKey(false);
    }

}
