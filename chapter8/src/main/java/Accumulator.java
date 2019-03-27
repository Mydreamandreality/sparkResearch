import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.org.apache.xpath.internal.SourceTree;
import org.apache.avro.ipc.specific.Person;
import org.apache.spark.InternalAccumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * Created by 張燿峰
 * 第八章案例
 * 累加器
 *
 * @author 孤
 * @date 2019/3/25
 * @Varsion 1.0
 */
public class Accumulator {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[4]").appName("AttackFind").getOrCreate();
        //初始化sparkContext
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        //日志输出级别
        javaSparkContext.setLogLevel("ERROR");
        //创建RDD
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList(JavaBean.origin_id, JavaBean.asset_name)).cache();

        AttackAccumulator attackAccumulator = new AttackAccumulator();
        //注册累加器
        javaSparkContext.sc().register(attackAccumulator, "attack_count");
        //生成一个随机数作为value
        JavaPairRDD<String, String> javaPairRDD = rdd.mapToPair((PairFunction<String, String, String>) s -> {
            Integer random = new Random().nextInt(10);
            return new Tuple2<>(s, s + ":" + random);
        });

        javaPairRDD.foreach((VoidFunction<Tuple2<String, String>>) tuple2 -> {
            attackAccumulator.add(tuple2._2);
        });
        System.out.println(attackAccumulator.value());
    }
}
