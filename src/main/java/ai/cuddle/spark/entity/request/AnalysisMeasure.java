package ai.cuddle.spark.entity.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Created by suman.das on 18/01/17.
 */
@JsonIgnoreProperties(
        ignoreUnknown = true
)
public class AnalysisMeasure implements Serializable {

    private static final long serialVersionUID = -4970260137898970904L;
    private String measureName;
    private List<AnalysisMeasureFilter> filters;

    public String getMeasureName() {
        return measureName;
    }

    public void setMeasureName(String measureName) {
        this.measureName = measureName;
    }

    public List<AnalysisMeasureFilter> getFilters() {
        return filters;
    }

    public void setFilters(List<AnalysisMeasureFilter> filters) {
        this.filters = filters;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisMeasure{");
        sb.append("measureName='").append(measureName).append('\'');
        sb.append(", filters=").append(filters);
        sb.append('}');
        return sb.toString();
    }
}
