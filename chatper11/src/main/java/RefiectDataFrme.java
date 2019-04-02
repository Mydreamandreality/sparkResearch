import dataset.Person;
import org.apache.ivy.osgi.p2.P2Artifact;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Encode;

/**
 * Created by 張燿峰
 * 第一种方式
 * 反射RDD转dataframe
 *
 * @author 孤
 * @date 2019/3/29
 * @Varsion 1.0
 */
public class RefiectDataFrme {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local")
                .appName("Java Spark SQL")
                .getOrCreate();

        //定义一个RDD,转换成DataFrame
        JavaRDD<Person> personJavaRDD = sparkSession.read().textFile("URL")
                .javaRDD().map(new Function<String, Person>() {
                    @Override
                    public Person call(String v1) {
                        String[] param = v1.split(":");
                        Person person = new Person();
                        person.setName(param[0]);
                        person.setAge(Integer.valueOf(param[1].trim()));
                        return person;
                    }
                });

        Dataset<Row> personDataset = sparkSession.createDataFrame(personJavaRDD,Person.class);
        //创建临时视图
        personDataset.createOrReplaceTempView("user");
        Dataset<Row> result = sparkSession.sql("SELECT * FROM user");
        //查看结果
        Encoder<String> encoder = Encoders.STRING();
        Dataset<String> dataset = result.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) {
                return value.getString(0);
            }
        },encoder);
        dataset.show();

        //第二种方式:通过字段获取value
        Dataset<String> fieldValue = result.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) {
                return value.getAs("name");
            }
        },encoder);
        fieldValue.show();
    }
}
