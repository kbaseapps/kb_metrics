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

    typedef string JobID;

    typedef int bool;
 
    /*
        A time range defined by its lower and upper bound.
    */
    typedef tuple<timestamp t_lowerbound, timestamp t_upperbound> time_range;
    typedef tuple<epoch e_lowerbound, epoch e_upperbound> epoch_range;
    typedef tuple<string ws_name, string narrative_name, int narrative_version> narrative_name_map;
    
    typedef structure {
        list<user_id> user_ids;
        epoch_range epoch_range;
        int offset;
        int limit;
    } AppMetricsParams;

    typedef structure {
        UnspecifiedObject job_states;
        int total_count;
    } AppMetricsResult;

    typedef structure {
        int ws_id;
        narrative_name_map narr_name_map;
    } MapWsNarrNamesResult;
  
    funcdef get_app_metrics(AppMetricsParams params)
        returns (AppMetricsResult return_records) authentication required;

    typedef structure {
        string app_id;
        list<string> client_groups;
        string user;
        bool complete;
        bool error;
        string status;
        int creation_time;
        int exec_start_time;
        int modification_time;
        int finish_time;
        JobID job_id;
        string method;
        string wsid;
        int narrative_objNo;
        string narrative_name;
        string workspace_name;
        bool narrative_is_deleted;
    } JobState;

    typedef structure {
        list<user_id> user_ids;
        epoch_range epoch_range;
        int offset;
        int limit;
    } GetJobsParams;

    typedef structure {
        list<JobState> job_states;
        int total_count;
    } GetJobsResult;

    funcdef get_jobs(GetJobsParams params)
        returns (GetJobsResult result) authentication required;

    /* Query jobs */

     typedef structure {
        JobID job_id;

        string app_id;
        string method; /* TODO: not sure why app and method */

        int workspace_id;
        int object_id;
        int object_version;

        string user;

        string status;
        bool complete;
        bool error;
        int creation_time;
        int exec_start_time;
        int finish_time;
        int modification_time;

        list<string> client_groups;
    } JobStateMinimal;

    typedef structure {
        string field;
        string direction;
    } SortSpec;

    typedef structure {
        string term;
        string type;
    } SearchSpec;

    typedef structure {
        list<string> job_id;
        list<user_id> user_id;
        list<string> status;
        list<int> workspace;
        list<string> app; 
    } FilterSpec;

    typedef structure {
        list<FilterSpec> filter;
        epoch_range epoch_range;
        list<SortSpec> sort;
        list<SearchSpec> search;
        int offset;
        int limit;
    } QueryJobsParams;

    typedef structure {
        list<JobStateMinimal> job_states;
        int total_count;
    } QueryJobsResult;

    funcdef query_jobs(QueryJobsParams params)
        returns (QueryJobsResult result) authentication required;

    typedef structure {
        list<FilterSpec> filter;
        epoch_range epoch_range;
        list<SortSpec> sort;
        list<SearchSpec> search;
        int offset;
        int limit;
    } QueryJobsAdminParams;

    typedef structure {
        list<JobStateMinimal> job_states;
        int total_count;
    } QueryJobsAdminResult;

    funcdef query_jobs_admin(QueryJobsAdminParams params)
        returns (QueryJobsAdminResult result) authentication required;

    /* Get an individual job by id */

    typedef structure {
        JobID job_id;
        user_id user_id;
    } GetJobParams;

    typedef structure {
        JobState job_state;
    } GetJobResult;

    funcdef get_job(GetJobParams params) 
        returns (GetJobResult result) authentication required;

    funcdef map_ws_narrative_names(list<int> ws_ids)
        returns (list<MapWsNarrNamesResult> return_records) authentication optional;

    /* unified input/output parameters*/
    typedef structure {
        list<user_id> user_ids;
        epoch_range epoch_range;
    } MetricsInputParams;

    typedef structure {
        UnspecifiedObject metrics_result;
    } MetricsOutput;
   
    /** For writing to mongodb metrics **/ 
    funcdef update_metrics(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    /** For retrieving from mongodb metrics **/ 
    funcdef get_user_details(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_nonkbuser_details(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_signup_returning_users(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_signup_returning_nonkbusers(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;
    
    funcdef get_user_counts_per_day(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_total_logins(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_nonkb_total_logins(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_user_logins(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;
    
    funcdef get_user_numObjs(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_narrative_stats(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_all_narrative_stats(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    funcdef get_user_ws_stats(MetricsInputParams params)
        returns (MetricsOutput return_records) authentication required;

    typedef structure {
        string username;
    } IsAdminParams;

    typedef structure {
        bool is_admin;
    } IsAdminResult;

    funcdef is_admin(IsAdminParams params)
        returns (IsAdminResult result) authentication required;
};
