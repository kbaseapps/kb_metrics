
package us.kbase.kbmetrics;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import us.kbase.common.service.UObject;


/**
 * <p>Original spec-file type: MetricsOutput</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "metrics_result"
})
public class MetricsOutput {

    @JsonProperty("metrics_result")
    private UObject metricsResult;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("metrics_result")
    public UObject getMetricsResult() {
        return metricsResult;
    }

    @JsonProperty("metrics_result")
    public void setMetricsResult(UObject metricsResult) {
        this.metricsResult = metricsResult;
    }

    public MetricsOutput withMetricsResult(UObject metricsResult) {
        this.metricsResult = metricsResult;
        return this;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        return ((((("MetricsOutput"+" [metricsResult=")+ metricsResult)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
