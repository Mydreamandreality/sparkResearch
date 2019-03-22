import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.ipc.specific.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 張燿峰
 * spark操作不同文件的案例
 *
 * @author 孤
 * @date 2019/3/22
 * @Varsion 1.0
 */
public class SaprkFile {

    public static void textFile(JavaSparkContext sparkContext) {

        //文本文件的读写
        JavaRDD<String> rdd = sparkContext.textFile("url");
        rdd.saveAsTextFile("url");
    }


    public static void jsonFile(JavaSparkContext sparkContext) {
        //json文件的读写
        JavaRDD<String> rdd = sparkContext.textFile("url");
        JavaRDD<Person> result = rdd.mapPartitions(new ParseJson()).filter(new filterData());
        result.saveAsTextFile("url");
    }

    static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
        @Override
        public Iterator<Person> call(Iterator<String> lines) {
            List<Person> arrayList = new ArrayList<>();
            ObjectMapper objectMapper = new ObjectMapper();
            while (lines.hasNext()) {
                try {
                    String line = lines.next();
                    arrayList.add(objectMapper.readValue(line, Person.class));
                } catch (IOException e) {
                    e.printStackTrace();
                    //此处失败跳过
                }
            }
            return arrayList.iterator();
        }
    }

    //写的JSON解析器
    static class wirteJson implements FlatMapFunction<Iterator<Person>, String> {
        @Override
        public Iterator<String> call(Iterator<Person> personIterator) {
            List<String> arrayList = new ArrayList<>();
            ObjectMapper objectMapper = new ObjectMapper();
            while (personIterator.hasNext()) {
                try {
                    arrayList.add(objectMapper.writeValueAsString(personIterator));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
            return arrayList.iterator();
        }
    }

    static class filterData implements Function<Person, Boolean> {
        @Override
        public Boolean call(Person v1) {
            return v1.get("filed").toString().contains("测试");
        }
    }
}
