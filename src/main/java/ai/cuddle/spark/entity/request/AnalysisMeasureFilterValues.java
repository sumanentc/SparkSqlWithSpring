package ai.cuddle.spark.entity.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Created by divyesheth on 18/01/17.
 */
@JsonIgnoreProperties(
        ignoreUnknown = true
)
public class AnalysisMeasureFilterValues implements Serializable{
    private AnalysisMeasureFilterArgumentTypeEnum argumentType;
    private String argument;

    public AnalysisMeasureFilterArgumentTypeEnum getArgumentType() {
        return argumentType;
    }

    public void setArgumentType(AnalysisMeasureFilterArgumentTypeEnum argumentType) {
        this.argumentType = argumentType;
    }

    public String getArgument() {
        return argument;
    }

    public void setArgument(String argument) {
        this.argument = argument;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisMeasureFilterValues{");
        sb.append("argumentType=").append(argumentType);
        sb.append(", argument='").append(argument).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
