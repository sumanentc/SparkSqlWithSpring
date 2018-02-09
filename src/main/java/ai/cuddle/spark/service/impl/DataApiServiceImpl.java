package ai.cuddle.spark.service.impl;

import ai.cuddle.spark.entity.MeasureDetail;
import ai.cuddle.spark.entity.request.Analysis;
import ai.cuddle.spark.entity.request.AnalysisAttribute;
import ai.cuddle.spark.entity.request.AttributeFilters;
import ai.cuddle.spark.service.AnalysisService;
import ai.cuddle.spark.service.DataApiService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by suman.das on 1/10/18.
 */
@Service
public class DataApiServiceImpl implements DataApiService {

    @Autowired
    AnalysisService analysisService;

    private String dateColumn ="the_date";

    @Override
    public Map<String, MeasureDetail> fetchMeasureDetails(List<String> measures) {
        Map<String,MeasureDetail> measureToFunctionMap = new HashMap<>();
        MeasureDetail m1 = new MeasureDetail();
        m1.setMeasureName("Total tdrimssales");
        m1.setCustomFunction(false);
        m1.setFunction("sum");
        m1.setFunctionArguments("tdrimssales");
        m1.setTransactionName("imssales_parque");

        MeasureDetail m2 = new MeasureDetail();
        m2.setMeasureName("Total stcimssales");
        m2.setCustomFunction(false);
        m2.setFunction("sum");
        m2.setFunctionArguments("stcimssales");
        m2.setTransactionName("imssales_parque");

        measureToFunctionMap.put("Total tdrimssales",m1);
        measureToFunctionMap.put("Total stcimssales",m2);

        return measureToFunctionMap;

    }

    @Override
    public List<Map<String, Object>> fetchData(Analysis analysis) {
        Map<String, MeasureDetail> measureDetailMap = fetchMeasureDetails(analysis.getMeasures().stream().map(analysisMeasure -> analysisMeasure.getMeasureName()).collect(Collectors.toList()));
        Analysis revisedAnalysis = checkDynamicEntity(analysis);
        return analysisService.fetchData(revisedAnalysis,measureDetailMap);
    }


    private Analysis checkDynamicEntity(Analysis analysis){
        Analysis revisedAnalysis = analysis;

        List<AnalysisAttribute> analysisAttributes = new ArrayList<>();

        for(AnalysisAttribute analysisAttribute : analysis.getAttributes()){
            if(analysisAttribute.getAttributeName().equalsIgnoreCase("today")){
                AnalysisAttribute attribute = new AnalysisAttribute();
                List<AttributeFilters> filters = new ArrayList<>();
                AttributeFilters filter1=new AttributeFilters();
                LocalDate today = LocalDate.now();
                filter1.setFilterValue(today.toString());
                filters.add(filter1);
                attribute.setAttributeName(dateColumn);
                attribute.setFilters(filters);
                attribute.setInGroupBy(false);
                analysisAttributes.add(attribute);
            }else if(analysisAttribute.getAttributeName().equalsIgnoreCase("yesterday")) {
                AnalysisAttribute attribute = new AnalysisAttribute();
                List<AttributeFilters> filters = new ArrayList<>();
                AttributeFilters filter1=new AttributeFilters();
                LocalDate today = LocalDate.now();
                filter1.setFilterValue(today.minusDays(1).toString());
                filters.add(filter1);
                attribute.setAttributeName(dateColumn);
                attribute.setFilters(filters);
                attribute.setInGroupBy(false);
                analysisAttributes.add(attribute);
            }else{
                analysisAttributes.add(analysisAttribute);
            }

        }
        revisedAnalysis.setAttributes(analysisAttributes);

        return revisedAnalysis;

    }
}
