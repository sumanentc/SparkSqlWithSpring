package ai.cuddle.spark.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

/**
 * Created by suman.das on 12/6/17.
 */
@Service
public class ParquetService implements Serializable {
    @Autowired
    private SparkSession sparkSession;

    private static Logger logger = Logger.getLogger(JDBCService.class);

    private ObjectMapper mapper = new ObjectMapper();

    public void saveData(){
        logger.info("inside fetch brandSales....");
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
            // DataFrames can be saved as Parquet files, maintaining the schema information
            sqlDF.write().parquet("/Users/suman.das/Documents/spark_data/imssales_flat.parquet");
        }catch(Exception e){
            e.printStackTrace();
        }

    }


    public void loadData()throws URISyntaxException{

                /*.format("com.springml.spark.sftp")
                                                 .option("host", "localhost")
                                                  .option("username", "suman.das")
                                                  .option("password", "fractal@123")
                                                  .option("delimiter", ",")
                                                  .option("fileType", "csv").
                                                   option("inferSchema", "true").
                                                   option("header", "true").
                                                    option("port","21").
                                                    load("/Users/suman.das/Public/newstoreglidepath.csv");*/
        //final URI ftpURI = new URI("ftp", "cuddleuser:Cu4", "ftp", 21, "/uploads/test.csv", null, null);
        Dataset<Row> data = sparkSession.read().format("csv").option("header","true").option("delimiter", ";").option("mode", "DROPMALFORMED").load("/Users/suman.das/Documents/hadoop/input/cuddle/td/test.csv");
                //.option("","")
                //.textFile("ftp://das:frac@localhost/Users/suman.das/Public/newstoreglidepath.csv");
        data.show(10);
        data.write().mode(SaveMode.Append).format("parquet").insertInto("test.newstoreglidepath");

        logger.info("Saving data done.....");
    }
}
