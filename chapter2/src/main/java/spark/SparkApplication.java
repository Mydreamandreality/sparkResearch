package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Created by 張燿峰
 * 第二章案例
 *
 * @author 孤
 * @date 2019/3/15
 * @Varsion 1.0
 */
public class SparkApplication {
    static SparkConf sparkConf = new SparkConf().setAppName("sparkBoot").setMaster("local");

    static JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        JavaRDD<String> lines = sparkContext.textFile("/usr/local/data").cache();

        lines.map(new Function<String, Object>() {
            @Override
            public Object call(String s) {
                return s;
            }
        });

        //生成多个输出
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordsOnes = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordsCounts = wordsOnes.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer value, Integer toValue) {
                return value + toValue;
            }
        });

        wordsCounts.saveAsTextFile("/usr/local/data1");
    }
}
//打包:mvn clean && mvn compile && mvn package
//我是有底线的--------------------------------
