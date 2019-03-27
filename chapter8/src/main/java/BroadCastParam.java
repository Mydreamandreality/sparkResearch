import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by 張燿峰
 * 广播变量案例
 * @author 孤
 * @date 2019/3/27
 * @Varsion 1.0
 */
public class BroadCastParam {

    /**
     * 广播变量测试
     * @param args
     */
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[4]").appName("AttackFind").getOrCreate();
        //初始化sparkContext
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        //在这里假定一份广播变量
        //因为我们之前说过,广播变量只可读
        final List<String> broadcastList = Arrays.asList("190099HJLL","98392QUEYY","561788LLKK");
        //设置广播变量,把broadcast广播出去
        final Broadcast<List<String>> broadcast = javaSparkContext.broadcast(broadcastList);
        //定义数据
        JavaPairRDD<String,String> pairRDD = javaSparkContext.parallelizePairs(Arrays.asList(new Tuple2<>("000", "000")));
        JavaPairRDD<String,String> resultPairRDD = pairRDD.filter((Function<Tuple2<String, String>, Boolean>) v1 -> broadcast.value().contains(v1._2));
        resultPairRDD.foreach((VoidFunction<Tuple2<String, String>>) System.out::println);
    }
}
