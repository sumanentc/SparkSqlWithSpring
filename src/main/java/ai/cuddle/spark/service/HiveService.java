package ai.cuddle.spark.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.*;

/**
 * Created by suman.das on 12/7/17.
 */
@Service
public class HiveService implements Serializable{
    @Autowired
    private SparkSession sparkSession;

    private static Logger logger = Logger.getLogger(JDBCService.class);

    private ObjectMapper mapper = new ObjectMapper();

    public void saveData(){
        logger.info(" saveData in Hive....");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "postgres");
        connectionProperties.put("driver", "org.postgresql.Driver");
        List<String> stringDataset= null;

        try {
            Dataset<Row> jdbcDF = sparkSession.read().jdbc("jdbc:postgresql://localhost:5432/olapstorage", "imssales_flat", connectionProperties);
            jdbcDF.printSchema();
            jdbcDF.createOrReplaceTempView("imssales_flat");
            Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM imssales_flat");
            sqlDF.show();
            sparkSession.sql("show databases ").show();
            sparkSession.sql("create table if not exists test.src as select * from imssales_flat where 1!=1");
            // sparkSession.sql("create table if not exists test.src using PARQUET PARTITIONED BY (yyyymmdd) as select * from imssales_flat where 1!=1");
            //sparkSession.sql("CREATE TABLE IF NOT EXISTS test.src (city STRING, cnt INT) OPTIONS(fileFormat 'parquet')");
            //sparkSession.sql("LOAD DATA LOCAL INPATH '/Users/suman.das/Downloads/city.csv' INTO TABLE test.src");
            sqlDF.write().format("parquet").insertInto("test.src");


        }catch(Exception e){
            e.printStackTrace();
        }

    }

    public List<Map<String,Object>> brandSales(){
        logger.info("inside fetch brandSales....");
        List<String> stringDataset= null;
        List<Map<String,Object>> result= new ArrayList<>();
        try {
            sparkSession.sql("show databases ").show();
            Dataset<Row> sqlDF = sparkSession.sql("SELECT tdrimssales,stcimssales,itemquantity,brand FROM test.imssales_parque where the_year=2017");
            sqlDF.createOrReplaceTempView("imssales_parque");
            System.out.println("cache -----> " + sparkSession.catalog().isCached("imssales_parque"));
            Dataset<Row> grpDataSet = sparkSession.sql("SELECT sum(tdrimssales) as tdrimssales ,sum(stcimssales) as stcimssales,sum(itemquantity) as itemquantity,brand FROM imssales_parque where brand='CARDINAL' group by brand");
            //RelationalGroupedDataset groupedDataset = sqlDF.groupBy(col("brand"));
            //Dataset<Row> grpDataSet = groupedDataset.agg(sum("tdrimssales").cast(DataTypes.createDecimalType(32,2)).alias("tdrimssales"),sum("stcimssales").cast(DataTypes.createDecimalType(32,2)).alias("stcimssales") ,sum("itemquantity").alias("item")).toDF();


            stringDataset = grpDataSet.toJSON().collectAsList();
            for(String s : stringDataset){
                Map<String,Object> map = new HashMap<>();
                map = mapper.readValue(s, new TypeReference<Map<String, String>>(){});
                result.add(map);
            }

        }catch(Exception e){
            e.printStackTrace();
        }

        return result;

    }

    public void buildCache(){
        Dataset<Row> sqlDF = sparkSession.sql("SELECT tdrimssales,stcimssales,itemquantity,brand FROM test.imssales_parque where the_year=2017");
        sqlDF.persist(StorageLevel.MEMORY_AND_DISK());
        sqlDF.createOrReplaceTempView("imssales_parque");
        sparkSession.sql("cache table imssales_parque");


    }

    public void clearCache(){
        sparkSession.catalog().clearCache();
    }

    public void loadCSV(){
        logger.info("loadCSV in Hive....");
        try {
            DataFrameReader dataFrameReader = sparkSession.read().schema(customSchema);
            Dataset<Row> responses = dataFrameReader.option("header","false").csv("file:///Users/suman.das/Downloads/imssales_nov_2017.csv");

            responses.createOrReplaceTempView("imssales_flat");
            Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM imssales_flat");
            sqlDF.show(20);
            sparkSession.sql("show databases ").show();
            sparkSession.sql("create table if not exists test.temp as select * from imssales_flat");
            sparkSession.sql("insert into test.imssales_flat_tab select * from test.temp");

            //sqlDF.write().format("orc").bucketBy(3,"yyyymmdd").saveAsTable("test.imssales_flat_tab");


        }catch(Exception e){
            e.printStackTrace();
        }

    }

        StructType customSchema = new StructType( new StructField[]{
        new StructField("itemquantity", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("tdrimssales", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("stcimssales", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("outletstatus", DataTypes.StringType, true, Metadata.empty()),
        new StructField("outletprogramtype", DataTypes.StringType, true, Metadata.empty()),
        new StructField("beat", DataTypes.StringType, true, Metadata.empty()),
        new StructField("outlet", DataTypes.StringType, true, Metadata.empty()),
        new StructField("outletlocationtype", DataTypes.StringType, true, Metadata.empty()),
        new StructField("outletclass", DataTypes.StringType, true, Metadata.empty()),
        new StructField("outletcategory", DataTypes.StringType, true, Metadata.empty()),
        new StructField("outlettype", DataTypes.StringType, true, Metadata.empty()),
        new StructField("zone", DataTypes.StringType, true, Metadata.empty()),
        new StructField("region", DataTypes.StringType, true, Metadata.empty()),
        new StructField("territory", DataTypes.StringType, true, Metadata.empty()),
        new StructField("area", DataTypes.StringType, true, Metadata.empty()),
        new StructField("tradesubtype", DataTypes.StringType, true, Metadata.empty()),
        new StructField("national", DataTypes.StringType, true, Metadata.empty()),
        new StructField("tradetype", DataTypes.StringType, true, Metadata.empty()),
        new StructField("dbsr", DataTypes.StringType, true, Metadata.empty()),
        new StructField("distributor", DataTypes.StringType, true, Metadata.empty()),
        new StructField("sku", DataTypes.StringType, true, Metadata.empty()),
        new StructField("micro", DataTypes.StringType, true, Metadata.empty()),
        new StructField("cpc", DataTypes.StringType, true, Metadata.empty()),
        new StructField("casesize", DataTypes.StringType, true, Metadata.empty()),
        new StructField("form", DataTypes.StringType, true, Metadata.empty()),
        new StructField("size", DataTypes.StringType, true, Metadata.empty()),
        new StructField("productclass", DataTypes.StringType, true, Metadata.empty()),
        new StructField("productsegment", DataTypes.StringType, true, Metadata.empty()),
        new StructField("manufacturer", DataTypes.StringType, true, Metadata.empty()),
        new StructField("brand", DataTypes.StringType, true, Metadata.empty()),
        new StructField("yymmdd", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("yyyymmdd", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("the_date", DataTypes.DateType, true, Metadata.empty()),
        new StructField("day_of_week", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("day_of_week_in_month", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("the_day", DataTypes.StringType, true, Metadata.empty()),
        new StructField("the_month", DataTypes.StringType, true, Metadata.empty()),
        new StructField("the_year", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("day_of_month", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("week_of_year", DataTypes.StringType, true, Metadata.empty()),
        new StructField("month_of_year", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("quarter", DataTypes.StringType, true, Metadata.empty())
        });





}
