# -*- coding: utf-8 -*-
#BEGIN_HEADER
# The header block is where all import statments should live
import os
from Bio import SeqIO
from pprint import pprint, pformat
from KBaseReport.KBaseReportClient import KBaseReport
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
    VERSION = "0.1.0"
    GIT_URL = "https://github.com/kbaseapps/kb_Metrics"
    GIT_COMMIT_HASH = "e35234875ff127b230f6fae3d087c22460d3ea2d"

    #BEGIN_CLASS_HEADER
    # Class variables and functions can be defined in this block
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR

        # Any configuration parameters that are important should be parsed and
        # saved in the constructor.
        self.ws_url = config['workspace-url']
        self.shared_folder = config['scratch']
        self.config = config
        self.mdb_controller = MetricsMongoDBController(self.config)
        #END_CONSTRUCTOR
        pass


    def get_app_metrics(self, ctx, params):
        """
        :param params: instance of type "AppMetricsParams" (job_stage has one
           of 'created', 'started', 'complete', 'canceled', 'error' or 'all'
           (default)) -> structure: parameter "user_ids" of list of type
           "user_id" (A string for the user id), parameter "epoch_range" of
           type "epoch_range" -> tuple of size 2: parameter "e_lowerbound" of
           type "epoch" (A Unix epoch (the time since 00:00:00 1/1/1970 UTC)
           in milliseconds.), parameter "e_upperbound" of type "epoch" (A
           Unix epoch (the time since 00:00:00 1/1/1970 UTC) in milliseconds.)
        :returns: instance of type "AppMetricsResult" -> structure: parameter
           "job_states" of unspecified object
        """
        # ctx is the context object
        # return variables are: return_records
        #BEGIN get_app_metrics
	return_records = self.mdb_controller.get_user_job_states(ctx['user_id'], params, ctx['token'])
        #END get_app_metrics

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_app_metrics return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_exec_apps(self, ctx, params):
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
        #BEGIN get_exec_apps
        return_records = self.mdb_controller.get_exec_apps(ctx['user_id'], params, ctx['token'])
        #END get_exec_apps

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_exec_apps return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_exec_tasks(self, ctx, params):
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
        #BEGIN get_exec_tasks
        return_records = self.mdb_controller.get_exec_tasks(ctx['user_id'], params, ctx['token'])
        #END get_exec_tasks

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_exec_tasks return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_details(self, ctx, params):
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
        #BEGIN get_user_details
        return_records = self.mdb_controller.get_user_details(ctx['user_id'], params, ctx['token'])
        #END get_user_details

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_details return value ' +
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
        return_records = self.mdb_controller.get_total_logins_from_ws(ctx['user_id'], params, ctx['token'])
        #END get_total_logins

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_total_logins return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_ws(self, ctx, params):
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
        #BEGIN get_user_ws
        return_records = self.mdb_controller.get_user_ws_from_ws(ctx['user_id'], params, ctx['token'])
        #END get_user_ws

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_ws return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_narratives(self, ctx, params):
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
        #BEGIN get_user_narratives
        return_records = self.mdb_controller.get_user_narratives_from_ws(ctx['user_id'], params, ctx['token'])
        #END get_user_narratives

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_narratives return value ' +
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
        return_records = self.mdb_controller.get_user_numObjs_from_ws(ctx['user_id'], params, ctx['token'])
        #END get_user_numObjs

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_numObjs return value ' +
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
        return_records = self.mdb_controller.get_user_logins_from_ws(ctx['user_id'], params, ctx['token'])
        #END get_user_logins

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_logins return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_ujs_results(self, ctx, params):
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
        #BEGIN get_user_ujs_results
        return_records = self.mdb_controller.get_ujs_results(ctx['user_id'], params, ctx['token'])
        #END get_user_ujs_results

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_ujs_results return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_job_states(self, ctx, params):
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
        #BEGIN get_user_job_states
        return_records = self.mdb_controller.get_user_job_states(ctx['user_id'], params, ctx['token'])
        #END get_user_job_states

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_job_states return value ' +
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
        return_records = self.mdb_controller.update_metrics(ctx['user_id'], params, ctx['token'])
        #END update_metrics

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method update_metrics return value ' +
                             'return_records is not type dict as required.')
        # return the results
        return [return_records]

    def get_user_activities(self, ctx, params):
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
        #BEGIN get_user_activities
        return_records = self.mdb_controller.get_user_activities(
					ctx['user_id'], params, ctx['token'])
        #END get_user_activities

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_activities return value ' +
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
        return_records = self.mdb_controller.get_active_users_counts(
					ctx['user_id'], params, ctx['token'])
        #END get_user_counts_per_day

        # At some point might do deeper type checking...
        if not isinstance(return_records, dict):
            raise ValueError('Method get_user_counts_per_day return value ' +
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
