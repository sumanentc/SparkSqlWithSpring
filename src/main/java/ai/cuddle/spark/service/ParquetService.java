package ai.cuddle.spark.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
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
}
