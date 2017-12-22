package ai.cuddle.spark.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Created by suman.das on 12/5/17.
 */
@Service
public class JDBCService implements Serializable{
    @Autowired
    private SparkSession sparkSession;

    private static Logger logger = Logger.getLogger(JDBCService.class);

    private ObjectMapper mapper = new ObjectMapper();


    /**
     * This method is used to the Employee table data
     * @return
     */
    public List<Map<String,Object>> fetchEmployeeData(){
        logger.info("inside fetch fetchEmployeeData....");
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "postgres");
        //connectionProperties.put("driver", "org.postgresql.Driver");
        List<String> stringDataset= null;
        List<Map<String,Object>> result= new ArrayList<>();

        try {
            Dataset<Row> jdbcDF = sparkSession.read().jdbc("jdbc:postgresql://localhost:5432/olapstorage", "employee", connectionProperties);
            jdbcDF.printSchema();
            jdbcDF.createOrReplaceTempView("employee");
            Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM employee");
            stringDataset=sqlDF.toJSON().collectAsList();
            logger.info("Result1 : " + stringDataset);
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

    /**
     * This method is used to the Employee details along with the Department having max salary.
     * This method is used for join testing
     * @return
     */
    public List<Map<String,Object>> fetchMaxSalaryEmployee(){
        logger.info("inside fetch fetchMaxSalaryEmployee....");
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "postgres");
        //connectionProperties.put("driver", "org.postgresql.Driver");
        List<String> stringDataset= null;
        List<Map<String,Object>> result= new ArrayList<>();

        try {
            Dataset<Row> jdbcDF = sparkSession.read().jdbc("jdbc:postgresql://localhost:5432/olapstorage", "employee", connectionProperties);
            jdbcDF.printSchema();
            jdbcDF.createOrReplaceTempView("employee");
            Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM employee");
            sqlDF.show();
            RelationalGroupedDataset groupedDataset = sqlDF.groupBy(col("dept"));

            Dataset<Row> grpDataSet = groupedDataset.agg(max("salary")).toDF("dept","salary");
            grpDataSet.show();

            Dataset<Row> finalDataSet=grpDataSet.join(sqlDF,sqlDF.col("salary").equalTo(grpDataSet.col("salary")));

            stringDataset = finalDataSet.toJSON().collectAsList();
            logger.info("Result1 : " + stringDataset);
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

    public List<Map<String,Object>> brandSales(){
        logger.info("inside fetch brandSales....");
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "postgres");
        connectionProperties.put("driver", "org.postgresql.Driver");
        List<String> stringDataset= null;
        List<Map<String,Object>> result= new ArrayList<>();
        try {
            Dataset<Row> jdbcDF = sparkSession.read()
                                            //.format("jdbc")
                                            //     .option("url", "jdbc:postgresql://localhost:5432/olapstorage")
                                            //     .option("dbtable", "imssales_flat")
                                            //     .option("user", "postgres")
                                            //     .option("password", "postgres")
                                            //     .option("driver", "org.postgresql.Driver")
                                            //     .option("fetchsize", "10000")
                                            //     .load();
                    .jdbc("jdbc:postgresql://localhost:5432/olapstorage", "imssales_flat", connectionProperties);
            jdbcDF.printSchema();
            jdbcDF.registerTempTable("imssales_flat");
            Dataset<Row> sqlDF = sparkSession.sql("SELECT tdrimssales,stcimssales,itemquantity,brand FROM imssales_flat where the_year=2017 and brand='CARDINAL'");

            RelationalGroupedDataset groupedDataset = sqlDF.groupBy(col("brand"));

            Dataset<Row> grpDataSet = groupedDataset.agg(sum("tdrimssales").cast(DataTypes.createDecimalType(32,2)).alias("tdrimssales"),sum("stcimssales").cast(DataTypes.createDecimalType(32,2)).alias("stcimssales") ,sum("itemquantity").alias("item")).toDF();
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

    public void loadCSV(){
        logger.info("inside fetch loadCSV....");
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "postgres");
        connectionProperties.put("driver", "org.postgresql.Driver");
        Dataset<Row> dataset = sparkSession.read().schema(customSchema).option("header","false").csv("file:///Users/suman.das/Downloads/imssales2017.csv");
        //Dataset<Row> dataset = sparkSession.read().option("header","true").csv("file:///Users/suman.das/Downloads/ims-sales-with-pk.csv");
        try {
            Dataset<Row> jdbcDF = sparkSession.read().jdbc("jdbc:postgresql://localhost:5432/olapstorage", "imssales_flat", connectionProperties);
            jdbcDF.printSchema();
            dataset.write().mode(SaveMode.Append).jdbc("jdbc:postgresql://localhost:5432/olapstorage","imssales_flat", connectionProperties);


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
