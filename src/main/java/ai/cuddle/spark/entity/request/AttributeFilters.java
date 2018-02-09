package ai.cuddle.spark.entity.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Created by suman.das on 4/19/17.
 */
@JsonIgnoreProperties(
        ignoreUnknown = true
)
public class AttributeFilters implements Serializable {
    private static final long serialVersionUID = -1367569255912803597L;
    private String filterValue;

    public String getFilterValue() {
        return filterValue;
    }

    public void setFilterValue(String filterValue) {
        this.filterValue = filterValue;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AttributeFilters{");
        sb.append("filterValue='").append(filterValue).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
