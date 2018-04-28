# -*- coding: utf-8 -*-
#BEGIN_HEADER
# The header block is where all import statments should live
from kb_Metrics.metricsdb_controller import MetricsMongoDBController
#END_HEADER


class kb_Metrics:
    '''
    Module Name:
    kb_Metrics

    Module Description:
    A KBase module: kb_Metrics
This KBase SDK module implements methods for generating various KBase metrics.
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "1.0.2"
    GIT_URL = "https://github.com/qzzhang/kb_Metrics"
    GIT_COMMIT_HASH = "10da8e1452b52f37b565700814d9902ba27948b5"

    #BEGIN_CLASS_HEADER
    # Class variables and functions can be defined in this block
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR

        # Any configuration parameters that are important should be parsed and
        # saved in the constructor.
        self.mdb_controller = MetricsMongoDBController(config)
        #END_CONSTRUCTOR
        pass


    def get_app_metrics(self, ctx, params):
        """
        :param params: instance of type "AppMetricsParams" -> structure:
           parameter "user_ids" of list of type "user_id" (A string for the
           user id), parameter "epoch_range" of type "epoch_range" -> tuple
           of size 2: parameter "e_lowerbound" of type "epoch" (A Unix epoch
           (the time since 00:00:00 1/1/1970 UTC) in milliseconds.),
           parameter "e_upperbound" of type "epoch" (A Unix epoch (the time
           since 00:00:00 1/1/1970 UTC) in milliseconds.)
        :returns: instance of type "AppMetricsResult" -> structure: parameter
           "job_states" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_app_metrics
        return_records = self.mdb_controller.get_user_job_states(ctx['user_id'],
                                                                 params,
                                                                 ctx['token'])
        #END get_app_metrics

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_app_metrics return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def update_metrics(self, ctx, params):
        """
        For writing to mongodb metrics *
        :param params: instance of type "MetricsInputParams" (unified
           input/output parameters) -> structure: parameter "user_ids" of
           list of type "user_id" (A string for the user id), parameter
           "epoch_range" of type "epoch_range" -> tuple of size 2: parameter
           "e_lowerbound" of type "epoch" (A Unix epoch (the time since
           00:00:00 1/1/1970 UTC) in milliseconds.), parameter "e_upperbound"
           of type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970
           UTC) in milliseconds.)
        :returns: instance of type "MetricsOutput" -> structure: parameter
           "metrics_result" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN update_metrics
        return_records = self.mdb_controller.update_metrics(ctx['user_id'],
                                                            params,
                                                            ctx['token'])
        #END update_metrics

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method update_metrics return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_details(self, ctx, params):
        """
        For retrieving from mongodb metrics *
        :param params: instance of type "MetricsInputParams" (unified
           input/output parameters) -> structure: parameter "user_ids" of
           list of type "user_id" (A string for the user id), parameter
           "epoch_range" of type "epoch_range" -> tuple of size 2: parameter
           "e_lowerbound" of type "epoch" (A Unix epoch (the time since
           00:00:00 1/1/1970 UTC) in milliseconds.), parameter "e_upperbound"
           of type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970
           UTC) in milliseconds.)
        :returns: instance of type "MetricsOutput" -> structure: parameter
           "metrics_result" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_user_details
        return_records = self.mdb_controller.get_user_details(ctx['user_id'],
                                                              params,
                                                              ctx['token'])
        #END get_user_details

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_details return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_signup_returning_users(self, ctx, params):
        """
        :param params: instance of type "MetricsInputParams" (unified
           input/output parameters) -> structure: parameter "user_ids" of
           list of type "user_id" (A string for the user id), parameter
           "epoch_range" of type "epoch_range" -> tuple of size 2: parameter
           "e_lowerbound" of type "epoch" (A Unix epoch (the time since
           00:00:00 1/1/1970 UTC) in milliseconds.), parameter "e_upperbound"
           of type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970
           UTC) in milliseconds.)
        :returns: instance of type "MetricsOutput" -> structure: parameter
           "metrics_result" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_signup_returning_users
        return_records = self.mdb_controller.get_signup_retn_users(ctx['user_id'],
                                                                   params,
                                                                   ctx['token'])
        #END get_signup_returning_users

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_signup_returning_users return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_counts_per_day(self, ctx, params):
        """
        :param params: instance of type "MetricsInputParams" (unified
           input/output parameters) -> structure: parameter "user_ids" of
           list of type "user_id" (A string for the user id), parameter
           "epoch_range" of type "epoch_range" -> tuple of size 2: parameter
           "e_lowerbound" of type "epoch" (A Unix epoch (the time since
           00:00:00 1/1/1970 UTC) in milliseconds.), parameter "e_upperbound"
           of type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970
           UTC) in milliseconds.)
        :returns: instance of type "MetricsOutput" -> structure: parameter
           "metrics_result" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_user_counts_per_day
        return_records = self.mdb_controller.get_active_users_counts(ctx['user_id'],
                                                                     params,
                                                                     ctx['token'])
        #END get_user_counts_per_day

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_counts_per_day return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_total_logins(self, ctx, params):
        """
        :param params: instance of type "MetricsInputParams" (unified
           input/output parameters) -> structure: parameter "user_ids" of
           list of type "user_id" (A string for the user id), parameter
           "epoch_range" of type "epoch_range" -> tuple of size 2: parameter
           "e_lowerbound" of type "epoch" (A Unix epoch (the time since
           00:00:00 1/1/1970 UTC) in milliseconds.), parameter "e_upperbound"
           of type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970
           UTC) in milliseconds.)
        :returns: instance of type "MetricsOutput" -> structure: parameter
           "metrics_result" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_total_logins
        return_records = self.mdb_controller.get_total_logins_from_ws(ctx['user_id'],
                                                                      params,
                                                                      ctx['token'])
        #END get_total_logins

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_total_logins return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_logins(self, ctx, params):
        """
        :param params: instance of type "MetricsInputParams" (unified
           input/output parameters) -> structure: parameter "user_ids" of
           list of type "user_id" (A string for the user id), parameter
           "epoch_range" of type "epoch_range" -> tuple of size 2: parameter
           "e_lowerbound" of type "epoch" (A Unix epoch (the time since
           00:00:00 1/1/1970 UTC) in milliseconds.), parameter "e_upperbound"
           of type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970
           UTC) in milliseconds.)
        :returns: instance of type "MetricsOutput" -> structure: parameter
           "metrics_result" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_user_logins
        return_records = self.mdb_controller.get_user_login_stats_from_ws(ctx['user_id'],
                                                                          params,
                                                                          ctx['token'])
        #END get_user_logins

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_logins return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_numObjs(self, ctx, params):
        """
        :param params: instance of type "MetricsInputParams" (unified
           input/output parameters) -> structure: parameter "user_ids" of
           list of type "user_id" (A string for the user id), parameter
           "epoch_range" of type "epoch_range" -> tuple of size 2: parameter
           "e_lowerbound" of type "epoch" (A Unix epoch (the time since
           00:00:00 1/1/1970 UTC) in milliseconds.), parameter "e_upperbound"
           of type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970
           UTC) in milliseconds.)
        :returns: instance of type "MetricsOutput" -> structure: parameter
           "metrics_result" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_user_numObjs
        return_records = self.mdb_controller.get_user_numObjs_from_ws(ctx['user_id'],
                                                                      params,
                                                                      ctx['token'])
        #END get_user_numObjs

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_numObjs return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_narrative_stats(self, ctx, params):
        """
        :param params: instance of type "MetricsInputParams" (unified
           input/output parameters) -> structure: parameter "user_ids" of
           list of type "user_id" (A string for the user id), parameter
           "epoch_range" of type "epoch_range" -> tuple of size 2: parameter
           "e_lowerbound" of type "epoch" (A Unix epoch (the time since
           00:00:00 1/1/1970 UTC) in milliseconds.), parameter "e_upperbound"
           of type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970
           UTC) in milliseconds.)
        :returns: instance of type "MetricsOutput" -> structure: parameter
           "metrics_result" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_narrative_stats
        return_records = self.mdb_controller.get_narrative_stats(ctx['user_id'],
                                                                 params,
                                                                 ctx['token'])
        #END get_narrative_stats

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_narrative_stats return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_ws_stats(self, ctx, params):
        """
        :param params: instance of type "MetricsInputParams" (unified
           input/output parameters) -> structure: parameter "user_ids" of
           list of type "user_id" (A string for the user id), parameter
           "epoch_range" of type "epoch_range" -> tuple of size 2: parameter
           "e_lowerbound" of type "epoch" (A Unix epoch (the time since
           00:00:00 1/1/1970 UTC) in milliseconds.), parameter "e_upperbound"
           of type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970
           UTC) in milliseconds.)
        :returns: instance of type "MetricsOutput" -> structure: parameter
           "metrics_result" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_user_ws_stats
        return_records = self.mdb_controller.get_user_ws_stats(ctx['user_id'],
                                                               params,
                                                               ctx['token'])
        #END get_user_ws_stats

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_ws_stats return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
