
package us.kbase.kbmetrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import us.kbase.common.service.Tuple2;


/**
 * <p>Original spec-file type: AppMetricsParams</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "user_ids",
    "epoch_range"
})
public class AppMetricsParams {

    @JsonProperty("user_ids")
    private List<String> userIds;
    @JsonProperty("epoch_range")
    private Tuple2 <Long, Long> epochRange;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("user_ids")
    public List<String> getUserIds() {
        return userIds;
    }

    @JsonProperty("user_ids")
    public void setUserIds(List<String> userIds) {
        this.userIds = userIds;
    }

    public AppMetricsParams withUserIds(List<String> userIds) {
        this.userIds = userIds;
        return this;
    }

    @JsonProperty("epoch_range")
    public Tuple2 <Long, Long> getEpochRange() {
        return epochRange;
    }

    @JsonProperty("epoch_range")
    public void setEpochRange(Tuple2 <Long, Long> epochRange) {
        this.epochRange = epochRange;
    }

    public AppMetricsParams withEpochRange(Tuple2 <Long, Long> epochRange) {
        this.epochRange = epochRange;
        return this;
    }

    @JsonAnyGetter
    public Map<java.lang.String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(java.lang.String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public java.lang.String toString() {
        return ((((((("AppMetricsParams"+" [userIds=")+ userIds)+", epochRange=")+ epochRange)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
