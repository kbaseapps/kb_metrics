/*
A KBase module: kb_Metrics
This KBase SDK module implements methods for generating various KBase metrics.
*/

module kb_Metrics {
    /********************************************************************************

        The following part is specifically dedicated to the dynamic json data service 
        
     ********************************************************************************/
    /*
        A string for the user id
    */
    typedef string user_id;
 
    /* 
        A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is the difference
        in time to UTC in the format +/-HHMM, eg:
                2012-12-17T23:24:06-0500 (EST time)
                2013-04-03T08:56:32+0000 (UTC time)
    */
    typedef string timestamp;
 
    /*
        A Unix epoch (the time since 00:00:00 1/1/1970 UTC) in milliseconds.
    */
    typedef int epoch;
 
    /*
        A time range defined by its lower and upper bound.
    */
    typedef tuple<timestamp t_lowerbound, timestamp t_upperbound> time_range;
    typedef tuple<epoch e_lowerbound, epoch e_upperbound> epoch_range;
    
    /*job_stage has one of 'created', 'started', 'complete', 'canceled', 'error' or 'all' (default)*/
    typedef structure {
        list<user_id> user_ids;
        epoch_range epoch_range;
    } AppMetricsParams;

    typedef structure {
        UnspecifiedObject job_states;
    } AppMetricsResult;
    
    funcdef get_app_metrics(AppMetricsParams params)
        returns (AppMetricsResult return_records) authentication required;

    /* unified input/output parameters*/
    typedef structure {
        list<user_id> user_ids;
        epoch_range epoch_range;
    } MetricsInputParams;

    typedef structure {
        UnspecifiedObject metrics_result;
    } MetricsOutput;
    
    funcdef get_exec_apps(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;
    
    funcdef get_exec_tasks(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;
    
    funcdef get_user_details(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_total_logins(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_user_ws(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_user_narratives(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_user_numObjs(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_user_logins(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_user_ujs_results(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;
    
    funcdef get_user_job_states(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;
   
    /** For writing to mongodb metrics **/ 
    funcdef update_metrics(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;
};
