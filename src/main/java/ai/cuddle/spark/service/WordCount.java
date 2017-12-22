package ai.cuddle.spark.service;

import ai.cuddle.spark.entity.Count;
import ai.cuddle.spark.entity.Word;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

/**
 * Created by suman.das on 11/30/17.
 */
@Component
public class WordCount implements Serializable{
    @Autowired
    private SparkSession sparkSession;

    private static Logger logger = Logger.getLogger(WordCount.class);

    private ObjectMapper mapper = new ObjectMapper();

    public List<Count> count() {
        String input = "hello world hello hello hello";
        String[] _words = input.split(" ");
        List<Word> words = Arrays.stream(_words).map(Word::new).collect(Collectors.toList());
        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, Word.class);
        dataFrame.show();
        //StructType structType = dataFrame.schema();

        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();

        return rows.stream().map(row -> new Count(row.getString(0), row.getLong(1))).collect(Collectors.toList());



        }




}
