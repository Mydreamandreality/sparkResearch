package datasource;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.SaveMode.Append;

/**
 * Created by 張燿峰
 * 数据源数据读取存储
 * @author 孤
 * @date 2019/4/2
 * @Varsion 1.0
 */
public class DataSources {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("spark app")
                .getOrCreate();

        Dataset<Row> rddDataset = sparkSession.read().parquet("usr/local/data.parquet");
        rddDataset.select("name","age").write().save("nameAndAge.parquet");

        Dataset<Row> jsonDataSet = sparkSession.read().json("usr/local/data.json");
        jsonDataSet.select("name","age").write().save("nameAndAge.json");

        //手动指定数据源

        Dataset<Row> customDataSource = sparkSession.read().format("json").load("usr/local/data.json");
        customDataSource.select("name","age").write().format("json").mode(SaveMode.Append).save("nameAndAge.json");
    }
}
