import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 張燿峰
 * 代码显示构造schema
 *
 * @author 孤
 * @date 2019/3/29
 * @Varsion 1.0
 */
public class CustomDataFrame {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("spark app")
                .getOrCreate();

        //创建普通的JavaRDD
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile("URL", 1).toJavaRDD();
        //字符串编码的模式
        String schema = "name age";

        //根据模式的字符串生成模式
        List<StructField> structFieldList = new ArrayList<>();
        for (String fieldName : schema.split(" ")) {
            StructField structField = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            structFieldList.add(structField);
        }
        StructType structType = DataTypes.createStructType(structFieldList);

        JavaRDD<Row> rowJavaRDD = javaRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) {
                String[] attirbutes = v1.split(",");
                return RowFactory.create(attirbutes[0], attirbutes[1].trim());
            }
        });

        //将模式应用于RDD
        Dataset<Row> dataset = sparkSession.createDataFrame(rowJavaRDD, structType);

        //创建临时视图
        dataset.createOrReplaceTempView("user");
        Dataset<Row> result = sparkSession.sql("select * from user");
        result.show();
    }
}
