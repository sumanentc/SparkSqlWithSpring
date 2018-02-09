package ai.cuddle.spark.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Created by suman.das on 1/8/18.
 */
@JsonIgnoreProperties
public class MeasureDetail implements Serializable{


    private static final long serialVersionUID = 3032656301361075368L;

    private String measureName;
    private String transactionName;
    private String function;
    private String functionArguments;
    private boolean isCustomFunction;
    private int precision;
    private String formatter;
    private String displayName;

    public String getMeasureName() {
        return measureName;
    }

    public void setMeasureName(String measureName) {
        this.measureName = measureName;
    }

    public String getTransactionName() {
        return transactionName;
    }

    public void setTransactionName(String transactionName) {
        this.transactionName = transactionName;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public String getFunctionArguments() {
        return functionArguments;
    }

    public void setFunctionArguments(String functionArguments) {
        this.functionArguments = functionArguments;
    }

    public boolean isCustomFunction() {
        return isCustomFunction;
    }

    public void setCustomFunction(boolean customFunction) {
        isCustomFunction = customFunction;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public String getFormatter() {
        return formatter;
    }

    public void setFormatter(String formatter) {
        this.formatter = formatter;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MeasureDetail{");
        sb.append("measureName='").append(measureName).append('\'');
        sb.append(", transactionName='").append(transactionName).append('\'');
        sb.append(", function='").append(function).append('\'');
        sb.append(", functionArguments='").append(functionArguments).append('\'');
        sb.append(", isCustomFunction=").append(isCustomFunction);
        sb.append(", precision=").append(precision);
        sb.append(", formatter='").append(formatter).append('\'');
        sb.append(", displayName='").append(displayName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
