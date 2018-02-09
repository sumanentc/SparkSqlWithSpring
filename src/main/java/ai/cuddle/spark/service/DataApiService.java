package ai.cuddle.spark.service;

import ai.cuddle.spark.entity.MeasureDetail;
import ai.cuddle.spark.entity.request.Analysis;

import java.util.List;
import java.util.Map;

/**
 * Created by suman.das on 1/10/18.
 */
public interface DataApiService {

    Map<String,MeasureDetail> fetchMeasureDetails(List<String> measures);

    List<Map<String,Object>> fetchData(Analysis analysis);


}
