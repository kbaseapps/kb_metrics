
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
 * <p>Original spec-file type: AppMetricsResult</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "job_states"
})
public class AppMetricsResult {

    @JsonProperty("job_states")
    private UObject jobStates;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("job_states")
    public UObject getJobStates() {
        return jobStates;
    }

    @JsonProperty("job_states")
    public void setJobStates(UObject jobStates) {
        this.jobStates = jobStates;
    }

    public AppMetricsResult withJobStates(UObject jobStates) {
        this.jobStates = jobStates;
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
        return ((((("AppMetricsResult"+" [jobStates=")+ jobStates)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
