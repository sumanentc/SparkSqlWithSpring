package ai.cuddle.spark.service;

import ai.cuddle.spark.entity.MeasureDetail;
import ai.cuddle.spark.entity.request.Analysis;
import ai.cuddle.spark.entity.request.AnalysisAttribute;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by suman.das on 1/3/18.
 */
@Service
public class AnalysisService {

    @Autowired
    private SparkSession sparkSession;

    private static Logger logger = Logger.getLogger(AnalysisService.class);

    public List<Map<String,Object>> fetchData(Analysis analysis,Map<String,MeasureDetail> measureDetailMap){
        List<Map<String,Object>> result = new ArrayList<>();
        /*
        analysis.getMeasures().forEach(analysisMeasure -> {
            queryBuilder.append(analysisMeasure.getMeasureName()).append(" ,");

        });*/

        try {

            Dataset<Row> dataset = fetchDataSet(analysis,measureDetailMap);
            List<String> stringDataset = dataset.toJSON().collectAsList();
            if(stringDataset!= null && !stringDataset.isEmpty()) {
                for (String s : stringDataset) {
                    result.add(convertToJSON(s));
                }
            }
            logger.info("Before Returning...");
        }catch (Exception e){
            logger.error("Exception occurred during fetch Data ",e);
        }

        return result;
    }


    private Dataset<Row> fetchDataSet(Analysis analysis,Map<String,MeasureDetail> measureDetailMap){
        StringBuilder queryBuilder = new StringBuilder();
        StringBuilder fromClause = new StringBuilder();
        StringBuilder havingClause = new StringBuilder();
        StringBuilder groupByClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();
        StringBuilder topBottom = new StringBuilder();
        queryBuilder.append("SELECT ");
        havingClause.append(" HAVING ");
        groupByClause.append(" GROUP BY  ");
        whereClause.append(" WHERE ");
        topBottom.append(" ORDER BY ");

        boolean whereClausePresent=false;
        boolean groupByClausePresent=false;
        boolean havingClausePresent=false;


        Set<String> transactionTables = new HashSet<>();
        Map<String,String> columnDataType = new HashMap<>();

        for(Map.Entry<String,MeasureDetail> entry : measureDetailMap.entrySet()){
            transactionTables.add(entry.getValue().getTransactionName());
            queryBuilder.append("cast (").append(entry.getValue().getFunction()).append("(").append(entry.getValue().getFunctionArguments()).append(") as decimal(32,").append("2").append("))").append(" as `").append(entry.getKey()).append("` ,");

        }


        transactionTables.forEach(transaction ->{
            try {
                Dataset<Column> columnDataset = sparkSession.catalog().listColumns(analysis.getClientName(), transaction);
                columnDataType.putAll(columnDataset.collectAsList().parallelStream().collect(Collectors.toMap(Column::name,Column::dataType)));
            }catch (AnalysisException a){
                logger.error("Exception occurred while fetching table schema for " + transaction,a);
            }
        });

        fromClause.append(" FROM ");

        for(String transactionTable : transactionTables){
            fromClause.append(analysis.getClientName()).append(".").append(transactionTable).append(" as ").append(transactionTable).append(",");

        }

        String tableAlias = transactionTables.stream().findFirst().get();

        for(AnalysisAttribute analysisAttribute : analysis.getAttributes()){
            if(analysisAttribute.isInGroupBy()){
                groupByClausePresent = true;

                queryBuilder.append(analysisAttribute.getAttributeName()).append(",");
                groupByClause.append(analysisAttribute.getAttributeName()).append(",");

                if (analysisAttribute.getFilters() != null && analysisAttribute.getFilters().size() > 0) {
                    String dataType = columnDataType.get(analysisAttribute.getAttributeName());
                    havingClausePresent=true;
                    havingClause.append(tableAlias).append(".").append(analysisAttribute.getAttributeName()).append(" in ( ");
                    analysisAttribute.getFilters().forEach(attributeFilters -> {
                        if(dataType.equalsIgnoreCase("STRING")){
                            havingClause.append("'").append(attributeFilters.getFilterValue()).append("',");
                        }else if(dataType.equalsIgnoreCase("INT")){
                            havingClause.append(attributeFilters.getFilterValue()).append(",");
                        }else if(dataType.equalsIgnoreCase("DATE")){
                            whereClause.append("'").append(attributeFilters.getFilterValue()).append("',");
                        }

                    });
                    havingClause.delete(havingClause.lastIndexOf(","),havingClause.length());
                    havingClause.append(" ) and ");
                }

            }else{
                whereClausePresent=true;

                if (analysisAttribute.getFilters() != null && analysisAttribute.getFilters().size() > 0) {
                    String dataType = columnDataType.get(analysisAttribute.getAttributeName());
                    whereClause.append(tableAlias).append(".").append(analysisAttribute.getAttributeName()).append(" in ( ");
                    analysisAttribute.getFilters().forEach(attributeFilters -> {
                        if(dataType.equalsIgnoreCase("STRING")){
                            whereClause.append("'").append(attributeFilters.getFilterValue()).append("',");
                        }else if(dataType.equalsIgnoreCase("INT")){
                            whereClause.append(attributeFilters.getFilterValue()).append(",");
                        }else if(dataType.equalsIgnoreCase("DATE")){
                            whereClause.append("'").append(attributeFilters.getFilterValue()).append("',");
                        }

                    });
                    whereClause.delete(whereClause.lastIndexOf(","),whereClause.length());
                    whereClause.append(" ) and ");
                }
            }


        }

        queryBuilder.delete(queryBuilder.lastIndexOf(","),queryBuilder.length());
        fromClause.delete(fromClause.lastIndexOf(","),fromClause.length());
        String query;
        if(whereClausePresent){
            whereClause.delete(whereClause.lastIndexOf("and"),whereClause.length());
            queryBuilder.append(fromClause).append(whereClause);
            if(groupByClausePresent){
                groupByClause.delete(groupByClause.lastIndexOf(","),groupByClause.length());
                queryBuilder.append(groupByClause);
            }
            if(havingClausePresent){
                havingClause.delete(havingClause.lastIndexOf("and"),havingClause.length());
                queryBuilder.append(havingClause);
            }
            query=queryBuilder.toString();
            logger.info("Final Query : " + query);
        }else{
            queryBuilder.append(fromClause);
            if(groupByClausePresent){
                groupByClause.delete(groupByClause.lastIndexOf(","),groupByClause.length());
                queryBuilder.append(groupByClause);
            }
            if(havingClausePresent){
                havingClause.delete(havingClause.lastIndexOf("and"),havingClause.length());
                queryBuilder.append(havingClause);
            }
            query=queryBuilder.toString();
            logger.info("Final Query : " + query);

        }

        Dataset<Row> sqlDataSet = sparkSession.sql(query);

        return sqlDataSet;

    }


    private List<String> fetchAggregatedData(String groupByColumns,Dataset<Row> dataset,Map<String,MeasureDetail> measureDetailMap){

        StringBuilder selectQuery = new StringBuilder();
        selectQuery.append("select ").append(groupByColumns);
        Map<String,String> aggreagtionMap = new HashMap<>();

        for(Map.Entry<String,MeasureDetail> entry : measureDetailMap.entrySet()){
            if(entry.getValue().isCustomFunction()){

            }else{
                aggreagtionMap.put(entry.getValue().getFunctionArguments(),entry.getValue().getFunction());
            }

        }
        Dataset<Row> groupedDataSet = dataset.groupBy(groupByColumns).agg(aggreagtionMap);
        return groupedDataSet.toJSON().collectAsList();

    }

    private Map<String,Object> convertToJSON(String tempJson){
        String[] parts = tempJson.split(",");
        Map<String,Object> jsonHash = new HashMap<String,Object>();
        for(int i=0;i<parts.length;i++){
            parts[i]    =   parts[i].replace("\"", "");
            parts[i]    =   parts[i].replace("{", "");
            parts[i]    =   parts[i].replace("}", "");
            String[] subparts = parts[i].split(":");
            if(subparts != null && subparts.length==2) {
                jsonHash.put(subparts[0], subparts[1]);
            }
        }
        return jsonHash;
    }
}
