# -*- coding: utf-8 -*-
#BEGIN_HEADER
# The header block is where all import statments should live
import os
from Bio import SeqIO
from pprint import pprint, pformat
from AssemblyUtil.AssemblyUtilClient import AssemblyUtil
from KBaseReport.KBaseReportClient import KBaseReport

from kb_Metrics.core.genome_feature_stats import genome_feature_stats
from kb_Metrics.core.report_utils import report_utils
from kb_Metrics.core.UJS_CAT_NJS_DataUtils import UJS_CAT_NJS_DataUtils
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
    VERSION = "0.0.1"
    GIT_URL = "https://github.com/kbaseapps/kb_Metrics.git"
    GIT_COMMIT_HASH = "904bb2b021e81c7431274bb4eb86b51f50dacfcb"

    #BEGIN_CLASS_HEADER
    # Class variables and functions can be defined in this block
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR

        # Any configuration parameters that are important should be parsed and
        # saved in the constructor.
        self.callback_url = os.environ['SDK_CALLBACK_URL']
        self.shared_folder = config['scratch']
        self.config = config
        #END_CONSTRUCTOR
        pass


    def count_ncbi_genome_features(self, ctx, params):
        """
        The actual function is declared using 'funcdef' to specify the name
        and input/return arguments to the function.  For all typical KBase
        Apps that run in the Narrative, your function should have the 
        'authentication required' modifier.
        :param params: instance of type "FeatureCountParams" (A 'typedef' can
           also be used to define compound or container objects, like lists,
           maps, and structures.  The standard KBase convention is to use
           structures, as shown here, to define the input and output of your
           function.  Here the input is a reference to the Assembly data
           object, a workspace to save output, and a length threshold for
           filtering. To define lists and maps, use a syntax similar to C++
           templates to indicate the type contained in the list or map.  For
           example: list <string> list_of_strings; mapping <string, int>
           map_of_ints;) -> structure: parameter "genbank_file_urls" of list
           of String, parameter "file_format" of String, parameter
           "genome_source" of String, parameter "genome_domain" of String,
           parameter "refseq_category" of String, parameter "workspace_name"
           of String, parameter "create_report" of type "bool" (A boolean - 0
           for false, 1 for true. @range (0, 1))
        :returns: instance of type "StatResults" (Here is the definition of
           the output of the function.  The output can be used by other SDK
           modules which call your code, or the output visualizations in the
           Narrative.  'report_name' and 'report_ref' are special output
           fields- if defined, the Narrative can automatically render your
           Report.) -> structure: parameter "report_name" of String,
           parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN count_ncbi_genome_features
        gfs = genome_feature_stats(self.config, ctx.provenance)

        output = gfs.count_ncbi_genome_features(params)
        #END count_ncbi_genome_features

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method count_ncbi_genome_features return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]

    def count_genome_features(self, ctx, params):
        """
        :param params: instance of type "FeatureCountParams" (A 'typedef' can
           also be used to define compound or container objects, like lists,
           maps, and structures.  The standard KBase convention is to use
           structures, as shown here, to define the input and output of your
           function.  Here the input is a reference to the Assembly data
           object, a workspace to save output, and a length threshold for
           filtering. To define lists and maps, use a syntax similar to C++
           templates to indicate the type contained in the list or map.  For
           example: list <string> list_of_strings; mapping <string, int>
           map_of_ints;) -> structure: parameter "genbank_file_urls" of list
           of String, parameter "file_format" of String, parameter
           "genome_source" of String, parameter "genome_domain" of String,
           parameter "refseq_category" of String, parameter "workspace_name"
           of String, parameter "create_report" of type "bool" (A boolean - 0
           for false, 1 for true. @range (0, 1))
        :returns: instance of type "StatResults" (Here is the definition of
           the output of the function.  The output can be used by other SDK
           modules which call your code, or the output visualizations in the
           Narrative.  'report_name' and 'report_ref' are special output
           fields- if defined, the Narrative can automatically render your
           Report.) -> structure: parameter "report_name" of String,
           parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN count_genome_features
        gfs = genome_feature_stats(self.config, ctx.provenance)

        output = gfs.count_genome_features(params)
        #END count_genome_features

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method count_genome_features return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]

    def refseq_genome_counts(self, ctx, params):
        """
        :param params: instance of type "GenomeCountParams" -> structure:
           parameter "genome_source" of String, parameter "genome_domain" of
           String, parameter "refseq_category" of String, parameter
           "workspace_name" of String, parameter "create_report" of type
           "bool" (A boolean - 0 for false, 1 for true. @range (0, 1))
        :returns: instance of type "StatResults" (Here is the definition of
           the output of the function.  The output can be used by other SDK
           modules which call your code, or the output visualizations in the
           Narrative.  'report_name' and 'report_ref' are special output
           fields- if defined, the Narrative can automatically render your
           Report.) -> structure: parameter "report_name" of String,
           parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN refseq_genome_counts
        gfs = genome_feature_stats(self.config, ctx.provenance)

        output = gfs.count_refseq_genomes(params)
        #END refseq_genome_counts

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method refseq_genome_counts return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]

    def report_metrics(self, ctx, params):
        """
        :param params: instance of type "StatsReportParams" -> structure:
           parameter "stats_name" of String, parameter "workspace_name" of
           String, parameter "create_report" of type "bool" (A boolean - 0
           for false, 1 for true. @range (0, 1))
        :returns: instance of type "StatResults" (Here is the definition of
           the output of the function.  The output can be used by other SDK
           modules which call your code, or the output visualizations in the
           Narrative.  'report_name' and 'report_ref' are special output
           fields- if defined, the Narrative can automatically render your
           Report.) -> structure: parameter "report_name" of String,
           parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN report_metrics
        rps = report_utils(self.config, ctx.provenance)

        #output = rps.get_module_stats_from_cat()
        output = rps.create_stats_report(params)
        #END report_metrics

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method report_metrics return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]

    def dummy_test0(self, ctx, params):
        """
        :param params: instance of type "StatsReportParams" -> structure:
           parameter "stats_name" of String, parameter "workspace_name" of
           String, parameter "create_report" of type "bool" (A boolean - 0
           for false, 1 for true. @range (0, 1))
        :returns: instance of type "StatResults" (Here is the definition of
           the output of the function.  The output can be used by other SDK
           modules which call your code, or the output visualizations in the
           Narrative.  'report_name' and 'report_ref' are special output
           fields- if defined, the Narrative can automatically render your
           Report.) -> structure: parameter "report_name" of String,
           parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN dummy_test0
        rps = report_utils(self.config, ctx.provenance)

        #output = rps.get_module_stats_from_cat()
        output = rps.create_stats_report(params)
        #END dummy_test0

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method dummy_test0 return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]

    def get_app_metrics(self, ctx, params):
        """
        :param params: instance of type "AppMetricsParams" (job_stage has one
           of 'created', 'started', 'complete', 'canceled', 'error' or 'all'
           (default)) -> structure: parameter "user_ids" of list of type
           "user_id" (A string for the user id), parameter "time_range" of
           type "time_range" (A time range defined by its lower and upper
           bound.) -> tuple of size 2: parameter "t_lowerbound" of type
           "timestamp" (A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is
           the difference in time to UTC in the format +/-HHMM, eg:
           2012-12-17T23:24:06-0500 (EST time) 2013-04-03T08:56:32+0000 (UTC
           time)), parameter "t_upperbound" of type "timestamp" (A time in
           the format YYYY-MM-DDThh:mm:ssZ, where Z is the difference in time
           to UTC in the format +/-HHMM, eg: 2012-12-17T23:24:06-0500 (EST
           time) 2013-04-03T08:56:32+0000 (UTC time)), parameter "job_stage"
           of String
        :returns: instance of type "AppMetricsResult" -> structure: parameter
           "job_states" of list of type "job_state" (Arbitrary key-value
           pairs about a job.) -> mapping from String to String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN get_app_metrics
        du = UJS_CAT_NJS_DataUtils(self.config, ctx.provenance)
        output = du.generate_app_metrics(params, ctx['token'])
        #END get_app_metrics

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method get_app_metrics return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]

    def get_user_metrics(self, ctx, params):
        """
        :param params: instance of type "UserMetricsParams" -> structure:
           parameter "filter_str" of String
        :returns: instance of type "UserMetricsResult" -> structure:
           parameter "user_metrics" of unspecified object
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN get_user_metrics
        du = UJS_CAT_NJS_DataUtils(self.config, ctx.provenance)
        output = du.generate_user_metrics(params, ctx['token'])
        #END get_user_metrics

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method get_user_metrics return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
