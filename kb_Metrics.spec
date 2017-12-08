/*
A KBase module: kb_Metrics
This KBase SDK module implements methods for generating various KBase metrics.
*/

module kb_Metrics {
    /* 
        A 'typedef' allows you to provide a more specific name for
        a type.  Built-in primitive types include 'string', 'int',
        'float'.  Here we define a type named assembly_ref to indicate
        a string that should be set to a KBase ID reference to an
        Assembly data object.
    */

    /* A boolean - 0 for false, 1 for true.
        @range (0, 1)
    */
                    
    typedef int bool;
                /* An X/Y/Z style reference
    */
    typedef string obj_ref;


    /*
        A 'typedef' can also be used to define compound or container
        objects, like lists, maps, and structures.  The standard KBase
        convention is to use structures, as shown here, to define the
        input and output of your function.  Here the input is a
        reference to the Assembly data object, a workspace to save
        output, and a length threshold for filtering.

        To define lists and maps, use a syntax similar to C++ templates
        to indicate the type contained in the list or map.  For example:

            list <string> list_of_strings;
            mapping <string, int> map_of_ints;
    */
    typedef structure {
        list <string> genbank_file_urls;
        string file_format;
        string genome_source;
        string genome_domain;
        string refseq_category;
        string workspace_name;
        bool create_report;
    } FeatureCountParams;


    /*
        Here is the definition of the output of the function.  The output
        can be used by other SDK modules which call your code, or the output
        visualizations in the Narrative.  'report_name' and 'report_ref' are
        special output fields- if defined, the Narrative can automatically
        render your Report.
    */
    typedef structure {
        string report_name;
        string report_ref;
    } StatResults;
    
    /*
        The actual function is declared using 'funcdef' to specify the name
        and input/return arguments to the function.  For all typical KBase
        Apps that run in the Narrative, your function should have the 
        'authentication required' modifier.
    */
    funcdef count_ncbi_genome_features(FeatureCountParams params)
        returns (StatResults output) authentication required;

    funcdef count_genome_features(FeatureCountParams params)
        returns (StatResults output) authentication required;


    typedef structure {
        string genome_source;
        string genome_domain;
        string refseq_category;
        string workspace_name;
        bool create_report;
    } GenomeCountParams;


    funcdef refseq_genome_counts(GenomeCountParams params)
        returns (StatResults output) authentication required;


    typedef structure {
        string stats_name;
        string workspace_name;
        bool create_report;
    } StatsReportParams;


    funcdef report_metrics(StatsReportParams params)
        returns (StatResults output) authentication required;

    funcdef dummy_test0(StatsReportParams params)
        returns (StatResults output) authentication required;

    /********************************************************************************

        The following part is specifically dedicated to the dynamic json data service 
        
     ********************************************************************************/
    /*
        An integer for the workspace id
    */
    typedef int ws_id;
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
    
    /*job_stage has one of 'created', 'started', 'complete', 'canceled', 'error' or 'all' (default)*/
    typedef structure {
        list<user_id> user_ids;
        time_range time_range;
        string job_stage; 
    } AppMetricsParams;

    /*
        Arbitrary key-value pairs about a job.
    */
    typedef mapping<string, string> job_state;

    typedef structure {
        list<job_state> job_states;
    } AppMetricsResult;
    
    funcdef get_app_metrics(AppMetricsParams params)
        returns (AppMetricsResult output) authentication required;


    typedef structure {
        string filter_str; 
        time_range time_range;
    } UserMetricsParams;

    typedef structure {
        UnspecifiedObject user_metrics;
    } UserMetricsResult;
    
    funcdef get_user_metrics(UserMetricsParams params)
        returns (UserMetricsResult output) authentication required;
    
    typedef structure {
        list<user_id> user_ids;
        int before;
        int after;
    } UserJobStatsParams;

    typedef structure {
        UnspecifiedObject user_tasks;
    } UserTasksResult;
    
    funcdef get_user_tasks(UserJobStatsParams params)
        returns (UserTasksResult ujs_records) authentication required;

    typedef structure {
        UnspecifiedObject user_details;
    } UserDetailsResult;
    
    funcdef get_user_details(UserJobStatsParams params)
        returns (UserDetailsResult ujs_records) authentication required;

    typedef structure {
        UnspecifiedObject user_job_states;
    } UserJobStatesResult;
    
    funcdef get_user_job_states(UserJobStatsParams params)
        returns (UserJobStatesResult ujs_records) authentication required;
};
