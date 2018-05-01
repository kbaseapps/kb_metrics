package us.kbase.kbmetrics;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonClientCaller;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.RpcContext;
import us.kbase.common.service.UnauthorizedException;

/**
 * <p>Original spec-file module name: kb_Metrics</p>
 * <pre>
 * A KBase module: kb_Metrics
 * This KBase SDK module implements methods for generating various KBase metrics.
 * </pre>
 */
public class KbMetricsClient {
    private JsonClientCaller caller;
    private String serviceVersion = null;


    /** Constructs a client with a custom URL and no user credentials.
     * @param url the URL of the service.
     */
    public KbMetricsClient(URL url) {
        caller = new JsonClientCaller(url);
    }
    /** Constructs a client with a custom URL.
     * @param url the URL of the service.
     * @param token the user's authorization token.
     * @throws UnauthorizedException if the token is not valid.
     * @throws IOException if an IOException occurs when checking the token's
     * validity.
     */
    public KbMetricsClient(URL url, AuthToken token) throws UnauthorizedException, IOException {
        caller = new JsonClientCaller(url, token);
    }

    /** Constructs a client with a custom URL.
     * @param url the URL of the service.
     * @param user the user name.
     * @param password the password for the user name.
     * @throws UnauthorizedException if the credentials are not valid.
     * @throws IOException if an IOException occurs when checking the user's
     * credentials.
     */
    public KbMetricsClient(URL url, String user, String password) throws UnauthorizedException, IOException {
        caller = new JsonClientCaller(url, user, password);
    }

    /** Constructs a client with a custom URL
     * and a custom authorization service URL.
     * @param url the URL of the service.
     * @param user the user name.
     * @param password the password for the user name.
     * @param auth the URL of the authorization server.
     * @throws UnauthorizedException if the credentials are not valid.
     * @throws IOException if an IOException occurs when checking the user's
     * credentials.
     */
    public KbMetricsClient(URL url, String user, String password, URL auth) throws UnauthorizedException, IOException {
        caller = new JsonClientCaller(url, user, password, auth);
    }

    /** Get the token this client uses to communicate with the server.
     * @return the authorization token.
     */
    public AuthToken getToken() {
        return caller.getToken();
    }

    /** Get the URL of the service with which this client communicates.
     * @return the service URL.
     */
    public URL getURL() {
        return caller.getURL();
    }

    /** Set the timeout between establishing a connection to a server and
     * receiving a response. A value of zero or null implies no timeout.
     * @param milliseconds the milliseconds to wait before timing out when
     * attempting to read from a server.
     */
    public void setConnectionReadTimeOut(Integer milliseconds) {
        this.caller.setConnectionReadTimeOut(milliseconds);
    }

    /** Check if this client allows insecure http (vs https) connections.
     * @return true if insecure connections are allowed.
     */
    public boolean isInsecureHttpConnectionAllowed() {
        return caller.isInsecureHttpConnectionAllowed();
    }

    /** Deprecated. Use isInsecureHttpConnectionAllowed().
     * @deprecated
     */
    public boolean isAuthAllowedForHttp() {
        return caller.isAuthAllowedForHttp();
    }

    /** Set whether insecure http (vs https) connections should be allowed by
     * this client.
     * @param allowed true to allow insecure connections. Default false
     */
    public void setIsInsecureHttpConnectionAllowed(boolean allowed) {
        caller.setInsecureHttpConnectionAllowed(allowed);
    }

    /** Deprecated. Use setIsInsecureHttpConnectionAllowed().
     * @deprecated
     */
    public void setAuthAllowedForHttp(boolean isAuthAllowedForHttp) {
        caller.setAuthAllowedForHttp(isAuthAllowedForHttp);
    }

    /** Set whether all SSL certificates, including self-signed certificates,
     * should be trusted.
     * @param trustAll true to trust all certificates. Default false.
     */
    public void setAllSSLCertificatesTrusted(final boolean trustAll) {
        caller.setAllSSLCertificatesTrusted(trustAll);
    }
    
    /** Check if this client trusts all SSL certificates, including
     * self-signed certificates.
     * @return true if all certificates are trusted.
     */
    public boolean isAllSSLCertificatesTrusted() {
        return caller.isAllSSLCertificatesTrusted();
    }
    /** Sets streaming mode on. In this case, the data will be streamed to
     * the server in chunks as it is read from disk rather than buffered in
     * memory. Many servers are not compatible with this feature.
     * @param streamRequest true to set streaming mode on, false otherwise.
     */
    public void setStreamingModeOn(boolean streamRequest) {
        caller.setStreamingModeOn(streamRequest);
    }

    /** Returns true if streaming mode is on.
     * @return true if streaming mode is on.
     */
    public boolean isStreamingModeOn() {
        return caller.isStreamingModeOn();
    }

    public void _setFileForNextRpcResponse(File f) {
        caller.setFileForNextRpcResponse(f);
    }

    public String getServiceVersion() {
        return this.serviceVersion;
    }

    public void setServiceVersion(String newValue) {
        this.serviceVersion = newValue;
    }

    /**
     * <p>Original spec-file function name: get_app_metrics</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.AppMetricsParams AppMetricsParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.AppMetricsResult AppMetricsResult}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public AppMetricsResult getAppMetrics(AppMetricsParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<AppMetricsResult>> retType = new TypeReference<List<AppMetricsResult>>() {};
        List<AppMetricsResult> res = caller.jsonrpcCall("kb_Metrics.get_app_metrics", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: update_metrics</p>
     * <pre>
     * * For writing to mongodb metrics *
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput updateMetrics(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.update_metrics", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_user_details</p>
     * <pre>
     * * For retrieving from mongodb metrics *
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getUserDetails(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_user_details", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_nonkbuser_details</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getNonkbuserDetails(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_nonkbuser_details", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_signup_returning_users</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getSignupReturningUsers(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_signup_returning_users", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_signup_returning_nonkbusers</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getSignupReturningNonkbusers(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_signup_returning_nonkbusers", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_user_counts_per_day</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getUserCountsPerDay(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_user_counts_per_day", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_total_logins</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getTotalLogins(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_total_logins", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_nonkb_total_logins</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getNonkbTotalLogins(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_nonkb_total_logins", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_user_logins</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getUserLogins(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_user_logins", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_user_numObjs</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getUserNumObjs(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_user_numObjs", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_narrative_stats</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getNarrativeStats(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_narrative_stats", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    /**
     * <p>Original spec-file function name: get_user_ws_stats</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link us.kbase.kbmetrics.MetricsInputParams MetricsInputParams}
     * @return   parameter "return_records" of type {@link us.kbase.kbmetrics.MetricsOutput MetricsOutput}
     * @throws IOException if an IO exception occurs
     * @throws JsonClientException if a JSON RPC exception occurs
     */
    public MetricsOutput getUserWsStats(MetricsInputParams params, RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        args.add(params);
        TypeReference<List<MetricsOutput>> retType = new TypeReference<List<MetricsOutput>>() {};
        List<MetricsOutput> res = caller.jsonrpcCall("kb_Metrics.get_user_ws_stats", args, retType, true, true, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }

    public Map<String, Object> status(RpcContext... jsonRpcContext) throws IOException, JsonClientException {
        List<Object> args = new ArrayList<Object>();
        TypeReference<List<Map<String, Object>>> retType = new TypeReference<List<Map<String, Object>>>() {};
        List<Map<String, Object>> res = caller.jsonrpcCall("kb_Metrics.status", args, retType, true, false, jsonRpcContext, this.serviceVersion);
        return res.get(0);
    }
}
