package ai.cuddle.spark.entity.request;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Created by divyesheth on 18/01/17.
 */
@JsonIgnoreProperties(
        ignoreUnknown = true
)
public class AnalysisMeasureFilter implements Serializable{

    private static final long serialVersionUID = 412891242128528151L;
    private String filter;
    private String conditionType;
    private List<AnalysisMeasureFilterValues> arguments;

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getConditionType() {
        return conditionType;
    }

    public void setConditionType(String conditionType) {
        this.conditionType = conditionType;
    }

    public List<AnalysisMeasureFilterValues> getArguments() {
        return arguments;
    }

    public void setArguments(List<AnalysisMeasureFilterValues> arguments) {
        this.arguments = arguments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisMeasureFilter{");
        sb.append("filter='").append(filter).append('\'');
        sb.append(", conditionType='").append(conditionType).append('\'');
        sb.append(", arguments=").append(arguments);
        sb.append('}');
        return sb.toString();
    }
}
