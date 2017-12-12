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

    typedef structure {
        string filter_str; 
        time_range time_range;
    } UserMetricsParams;

    typedef structure {
        UnspecifiedObject user_metrics;
    } UserMetricsResult;
    
    funcdef get_user_metrics(UserMetricsParams params)
        returns (UserMetricsResult return_records) authentication required;
    
    typedef structure {
        list<user_id> user_ids;
        epoch_range epoch_range;
    } UserJobStatsParams;

    typedef structure {
        UnspecifiedObject user_apps;
    } ExecAppsResult;
    
    funcdef get_exec_apps(UserJobStatsParams params)
        returns (ExecAppsResult return_records) authentication required;

    typedef structure {
        UnspecifiedObject user_tasks;
    } ExecTasksResult;
    
    funcdef get_exec_tasks(UserJobStatsParams params)
        returns (ExecTasksResult return_records) authentication required;

    typedef structure {
        UnspecifiedObject user_details;
    } UserDetailsResult;
    
    funcdef get_user_details(UserJobStatsParams params)
        returns (UserDetailsResult return_records) authentication required;

    typedef structure {
        UnspecifiedObject ujs_results;
    } UserJobStatesResult;

    funcdef get_user_ujs_results(UserJobStatsParams params)
        returns (UserJobStatesResult return_records) authentication required;
    
    funcdef get_user_job_states(UserJobStatsParams params)
        returns (UserJobStatesResult return_records) authentication required;
};
