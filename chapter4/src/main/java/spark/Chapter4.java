package spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Created by 張燿峰
 * Spark常见案例
 *
 * @author 孤
 * @date 2019/3/15
 * @Varsion 1.0
 */
public class Chapter4 {
    private static final Pattern PATTERN = Pattern.compile(" ");

    /**
     * 计算RDD中各值的平方
     */
    public void map(JavaSparkContext sparkContext) {
        JavaRDD<Integer> num = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));

        //新生成的RDD元素
        JavaRDD<Integer> result = num.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * v1;
            }
        });
        System.out.println(StringUtils.join(result.collect(),","));
    }

    /**
     * flatMap分割字符串
     */
    public void flatMap(JavaSparkContext sparkContext){
        JavaRDD<String> lines = sparkContext.parallelize(Arrays.asList("hello world", "hi"));

        JavaRDD<String> flatMapResult  = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(PATTERN.split(s)).iterator();
            }
        });

        flatMapResult.first();

        //结果:hello
    }

    public void reduce(JavaSparkContext sparkContext){
        JavaRDD<Integer> lines = sparkContext.parallelize(Arrays.asList(1,2,3,4));
        JavaRDD<Integer> toLines = sparkContext.parallelize(Arrays.asList(1,2,3,4));

    }
}
