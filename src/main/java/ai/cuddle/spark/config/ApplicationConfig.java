package ai.cuddle.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * Created by suman.das on 11/30/17.
 */
@Configuration
public class ApplicationConfig {

    @Value("${app.name:test}")
    private String appName;

    @Value("${spark.home}")
    private String sparkHome;

    @Value("${master.uri:local[*]}")
    private String masterUri;

    @Bean
    public SparkConf sparkConf() {
        //SparkConf sparkConf = new SparkConf().setAppName(appName).setSparkHome(sparkHome).setMaster(masterUri);
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(masterUri);

        return sparkConf;
    }

    //@Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                //.sparkContext(javaSparkContext().sc())
                //.config("spark.sql.warehouse.dir","/Users/suman.das/Documents/spark/data")
                .config(sparkConf())
                .config("spark.sql.warehouse.dir","/Users/suman.das/Documents/hive/user/hive/warehouse")
                .config("hive.metastore.uris","thrift://localhost:9083")
                .config("spark.sql.hive.convertMetastoreOrc","false")
                .enableHiveSupport()
                .getOrCreate();

    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}
