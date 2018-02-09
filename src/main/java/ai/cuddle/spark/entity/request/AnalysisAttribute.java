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
public class AnalysisAttribute implements Serializable {

    private static final long serialVersionUID = 1190901743626774117L;
    private String attributeName;
    private List<AttributeFilters> filters;
    private boolean inGroupBy = false;

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public List<AttributeFilters> getFilters() {
        return filters;
    }

    public void setFilters(List<AttributeFilters> filters) {
        this.filters = filters;
    }

    public boolean isInGroupBy() {
        return inGroupBy;
    }

    public void setInGroupBy(boolean inGroupBy) {
        this.inGroupBy = inGroupBy;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisAttribute{");
        sb.append("attributeName='").append(attributeName).append('\'');
        sb.append(", filters=").append(filters);
        sb.append(", inGroupBy=").append(inGroupBy);
        sb.append('}');
        return sb.toString();
    }
}
