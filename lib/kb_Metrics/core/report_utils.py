import time
import datetime
import json
import os
import re
import copy
import uuid
import subprocess
import shutil
import sys
import zipfile
import gzip
from pprint import pprint, pformat
from urllib2 import Request, urlopen
from urllib2 import URLError, HTTPError
import urllib
import errno

from Bio import Entrez, SeqIO
from numpy import median, mean, max

from Workspace.WorkspaceClient import Workspace as Workspace
from KBaseReport.KBaseReportClient import KBaseReport
from Catalog.CatalogClient import Catalog
from NarrativeJobService.NarrativeJobServiceClient import NarrativeJobService
from UserAndJobState.UserAndJobStateClient import UserAndJobState
#import biokbase.narrative.clients as clients
#from biokbase.catalog.Client import Catalog

def log(message, prefix_newline=False):
    """Logging function, provides a hook to suppress or redirect log messages."""
    print(('\n' if prefix_newline else '') + '{0:.2f}'.format(time.time()) + ': ' + str(message))


def _mkdir_p(path):
    """
    _mkdir_p: make directory for given path
    """
    if not path:
        return
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def ceildiv(a, b):
    """
    celldiv: get the ceiling division of two integers, by reversing the floor division
    """
    return -(-a // b)

def _datetime_from_utc(date_utc_str):
    return datetime.datetime.strptime(date_utc_str,'%Y-%m-%dT%H:%M:%S+0000')

def _timestamp_from_utc(date_utc_str):
    dt = _datetime_from_utc(date_utc_str)
    return int(time.mktime(dt.timetuple())*1000) #in microseconds

class report_utils:
    PARAM_IN_WS = 'workspace_name'

    def __init__(self, config, provenance):
        self.workspace_url = config['workspace-url']
        self.callback_url = os.environ['SDK_CALLBACK_URL']
        self.token = os.environ['KB_AUTH_TOKEN']
        self.provenance = provenance

        self.scratch = os.path.join(config['scratch'], str(uuid.uuid4()))
        _mkdir_p(self.scratch)

        if 'shock-url' in config:
            self.shock_url = config['shock-url']
        if 'handle-service-url' in config:
            self.handle_url = config['handle-service-url']

        self.ws_client = Workspace(self.workspace_url, token=self.token)
        #self.cat_client = Catalog(self.callback_url)
        self.cat_client = Catalog('https://kbase.us/services/catalog', auth_svc='https://kbase.us/services/auth/')
        self.njs_client = NarrativeJobService('https://kbase.us/services/njs_wrapper', auth_svc='https://kbase.us/services/auth/')
        self.ujs_client = UserAndJobState('https://kbase.us/services/userandjobstate', auth_svc='https://kbase.us/services/auth/')
        self.kbr = KBaseReport(self.callback_url)
        self.metrics_dir = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(self.metrics_dir)


    def generate_app_metrics(self, params):
        """
        """
        if params.get(self.PARAM_IN_WS, None) is None:
           raise ValueError(self.PARAM_IN_WS + ' parameter is mandatory')

        if params.get('ws_ids', None) is None:
            raise ValueError('Variable ws_ids' + ' parameter is mandatory')
        if not isinstance(params['ws_ids'], list):
            raise ValueError('Variable ws_ids' + ' must be a list.')
        if not params['ws_ids']:
            raise ValueError('At least one workspace id must be provided')

        if not params.get('time_range', None) is None:
            time_start, time_end = params['time_range']

        ws_ids = [str(wd) for wd in params['ws_ids']] #e.g.ws_ids = [str(25735), str(25244)]
        ujs_ret = self.get_user_and_job_states(ws_ids)
        #log("Before time frame: {}".format(len(ujs_ret)))

        ret_val = []
        for ujs_i in ujs_ret:
            if isinstance(ujs_i['creation_time'], int):
                cr_time = datetime.datetime.utcfromtimestamp(ujs_i['creation_time']/1000)
            else:
                cr_time = _datetime_from_utc(ujs_i['creation_time'])

            if (cr_time <= _datetime_from_utc(time_end) and
                        cr_time >= _datetime_from_utc(time_start)):
                ret_val.append(ujs_i)

        #log("After time frame: {}".format(len(ret_val)))
        log(pformat(ret_val[100]))

        returnVal = {
            'report_name': None,
            'report_ref': None
        }

        if params['create_report'] == 1:
            report_info = self.generate_app_report(self.metrics_dir, ret_val, params)

            returnVal = {
                'report_name': report_info['name'],
                'report_ref': report_info['ref']
            }

        return returnVal


    def create_stats_report(self, params):
        """
        """
        if params.get(self.PARAM_IN_WS, None) is None:
           raise ValueError(self.PARAM_IN_WS + ' parameter is mandatory')

        if params.get('stats_name', None) is None:
            raise ValueError('Variable stats_name' + ' parameter is mandatory')

        stats_name = params['stats_name']

        cat_calls = {
            'exec_stats': 'get_exec_stats_from_cat',
            'exec_aggr_stats': 'get_exec_aggrStats_from_cat',
            'exec_aggr_table': 'get_exec_aggrTable_from_cat'
        }
        ret_stats = []
        if stats_name == 'exec_stats':
            ret_stats = self.get_exec_stats_from_cat()
        elif stats_name == 'exec_aggr_stats':
            ret_stats = self.get_exec_aggrStats_from_cat()
        elif stats_name == 'exec_aggr_table':
            ret_stats = self.get_exec_aggrTable_from_cat()
        elif stats_name == 'user_job_states':
            #ws_ids = self.get_user_workspaces(['qzhang'])
            #log("\nExclude deleted {} workspaces".format(len(ws_ids)))
            #ws_ids2 = self.get_user_workspaces(['qzhang'], 1, 1)
            #log("\nOnly deleted {} workspaces".format(len(ws_ids2)))
            ws_ids = [str(25735), str(25244)]
            ret_stats = self.get_user_and_job_states(ws_ids)
        else:
            pass

        returnVal = {
            "report_ref": None,
            "report_name": None
        }

        if len(ret_stats) == 0:
            return returnVal

        col_caps = ['module_name', 'full_app_id', 'number_of_calls', 'number_of_errors',
                        'type', 'time_range', 'total_exec_time', 'total_queue_time']
        if params['create_report'] == 1:
            report_info = self.generate_report(self.metrics_dir, ret_stats, params)
            #report_info = self.generate_report(self.metrics_dir, raw_stats, params)
            #report_info = self.generate_report(self.metrics_dir, aggr_stats, params, col_caps)

            returnVal = {
                'report_name': report_info['name'],
                'report_ref': report_info['ref']
            }

        return returnVal


    def get_user_workspaces(self, user_ids, showDeleted=0, showOnlyDeleted=0):
        """
        get_user_workspaces: given the user ids, retrieve a list of data structure as the example below:
        typedef tuple<ws_id id,
              ws_name workspace,
              username owner,
              timestamp moddate,
              int max_objid,
              permission user_permission,
              permission globalread,
              lock_status lockstat,
              usermeta metadata> workspace_info;

        return a list of ws_ids
        """
        ws_info = self.ws_client.list_workspace_info({'owners':user_ids,
                        'showDeleted': showDeleted,
                        'showOnlyDeleted': showOnlyDeleted,
                        'perm':'r',
                        'excludeGlobal': 1,
                        'after': '2017-04-03T08:56:32Z',
                        'before': '2017-11-03T08:56:32Z'
                })
        #ws_info = self.ws_client.list_workspace_info({'meta': {'is_temporary': u'false'}})

        #log(pformat(ws_info))

        ws_ids = [ws[0] for ws in ws_info]
        ws_names = [ws[1] for ws in ws_info]

        #log(pformat(ws_ids))

        return ws_ids


    def get_user_and_job_states(self, ws_ids):
        """
        """
        # Pull the data
        log("Fetching the data from NarrativeJobService API...")
        #log(pformat(ws_ids))
        j_states = []
        clnt_groups = self.get_client_groups_from_cat()

        for idx, wid in enumerate(ws_ids):
            batch_js = self.retrieve_user_job_states([wid], clnt_groups)
            if len(batch_js) > 0:
                j_states = j_states + batch_js

        #log(pformat(j_states[0]))
        #log(pformat(j_states))

        return j_states


    def retrieve_user_job_states(self, wid_p, c_groups):
        """
        call ujs_client.list_jobs2() that returns an array of job_info2:
        typedef tuple<job_id job, user_info users, service_name service,
                job_stage stage, job_status status, time_info times,
                progress_info progress, boolean complete, boolean error,
                auth_info auth, usermeta meta, job_description desc, Results res>
                job_info2;

        retrieve_user_job_states: returns an array of required data items about user_and_job states
        """
        log("Fetching the data from UserAndJobState API for workspace(s) {}...".format(pformat(wid_p)))
        ret_ujs = []
        try:
            nar_jobs = self.ujs_client.list_jobs2({
                'authstrat': 'kbaseworkspace',
                'authparams': wid_p
            })
        except Exception as e_ujs: #RuntimeError as e_ujs:
            log('UJS list_jobs2 raised error:\n')
            log(pformat(e_ujs))
            return []
        else:#no exception raised
            if (nar_jobs and len(nar_jobs) > 0):
                #******The ujs_client.list_jobs2({...}) returns a 13 member tuple:*****#
                job_ids = [j[0] for j in nar_jobs]#[u'59f36d00e4b0fb0c767100cc',...]
                job_user_info = [j[1] for j in nar_jobs]#[[u'qzhang', None],[u'qzhang', u'qzhang'],...]
                job_owners = [j[2] for j in nar_jobs]#[u'qzhang',u'qzhang',...]
                job_stages = [j[3] for j in nar_jobs]#One of 'created', 'started', 'completed', 'canceled' or 'error'
                job_status = [j[4] for j in nar_jobs]##[u'done','running','canceled by user','......',...]
                job_time_info = [j[5] for j in nar_jobs]#tuple<timestamp started, timestamp last_update,timestamp est_complete>[[u'2017-10-27T17:29:37+0000', u'2017-10-27T17:29:42+0000', None],...]
                job_progress_info = [j[6] for j in nar_jobs]#tuple<total_progress prog, max_progress max, progress_type ptype>
                job_complete = [j[7] for j in nar_jobs]#[1,1,...,0,..]
                job_error = [j[8] for j in nar_jobs]#[1,0,...,0,..]
                job_auth_info = [j[9] for j in nar_jobs]#[[u'kbaseworkspace', u'25735'],...]
                job_meta = [j[10] for j in nar_jobs]#[{u'cell_id': u'828d2e3c-5c5d-4c4c-9de8-4aacb875c074',u'run_id': u'a05df5b3-2d3e-4e4a-9a32-173acaa9bd0c',u'tag': u'beta',u'token_id': u'2dea84eb-8f40-4516-b18e-f284cc6bb107'},...]
                job_desc = [j[11] for j in nar_jobs]#[u'Execution engine job for kb_Metrics.count_ncbi_genome_features',...]
                job_res = [j[12] for j in nar_jobs]#[{},None,...]

                njs_ret = self.retrieve_ujs_via_njs(c_groups, job_ids, job_owners,
                                job_stages, job_status, job_time_info, job_error, job_desc)
                if njs_ret:
                    ret_ujs = njs_ret

        return ret_ujs

    def retrieve_ujs_via_njs(self, c_groups, job_ids, job_owners, job_stages,
                        job_status, job_time_info,job_error, job_desc):
        ujs_ret = []
        try:
            #log("Calling njs.check_jobs for {} jobs".format(len(job_ids)))
            job_info = self.njs_client.check_jobs({
                        'job_ids': job_ids, 'with_job_params': 1
                })
            #log("njs returned {} job_info".format(len(job_info.get('job_states', {}))))
        except Exception as e_njs: #RuntimeError as e_njs:
            log('NJS check_jobs raised error:\n')
            log(pformat(e_njs))
            return []
        else:#no exception raised
            job_states = job_info.get('job_states', {})
            job_params = job_info.get('job_params', {})
            job_errors = job_info.get('check_error', {})

            # Retrieve the interested data from job_states to assemble an array of job states
            #for j_id, j_owner in zip(job_ids, job_owners):
            for j_idx, jb_id in enumerate(job_ids):
                jbs = job_states.get(job_ids[j_idx], {})
                jbp = job_params.get(job_ids[j_idx], {})
                u_j_s = {}
                u_j_s['job_id'] = job_ids[j_idx]
                u_j_s['user_id'] = job_owners[j_idx]
                u_j_s['status'] = job_status[j_idx]
                u_j_s['stage'] = job_stages[j_idx]
                u_j_s['time_info'] = job_time_info[j_idx]
                u_j_s['error'] = job_error[j_idx]
                u_j_s['job_desc'] = job_desc[j_idx]

                if jbs:
                    try:
                        u_j_s['app_id'] = jbp['app_id']
                        for clnt in c_groups:
                            if u_j_s['app_id'] == clnt['app_id']:
                                u_j_s['client_groups'] = clnt['client_groups']
                                break
                        u_j_s['wsid'] = jbp['wsid']
                        u_j_s['module'], u_j_s['method'] = jbp['method'].split('.')
                        u_j_s['job_state'] = jbs['job_state']
                        if jbs['job_state'] == 'suspend':
                            u_j_s['error'] = jbs['error']
                        elif jbs['job_state'] == 'completed':
                            u_j_s['result'] = jbs['result']

                        u_j_s['finished'] = jbs['finished']
                        u_j_s['canceled'] = jbs['canceled']
                        u_j_s['creation_time'] = jbs['creation_time']
                        if 'exec_start_time' in jbs:
                            u_j_s['exec_start_time'] = jbs['exec_start_time']
                        elif u_j_s['stage'] == 'started':
                            u_j_s['exec_start_time'] = u_j_s['time_info'][1]
                        if 'finish_time' in jbs:
                            u_j_s['finish_time'] = jbs['finish_time']
                        elif u_j_s['stage'] == 'completed':
                            u_j_s['finish_time'] = u_j_s['time_info'][1]
                    except KeyError as e_key:
                        log("KeyError for " + pformat(e_key))
                    else:
                        pass
                else:
                    #log("No job state info is returned by njs for job with id {}".format(job_ids[j_idx]))
                    #log("\nBut maybe ujs has returned something for job with id {}".format(job_ids[j_idx]))
                    #log(pformat(job_stages[j_idx]))
                    u_j_s['creation_time'] = _timestamp_from_utc(u_j_s['time_info'][0])
                    if (u_j_s['stage'] == 'started' and u_j_s['status'] == 'running'):
                        u_j_s['exec_start_time'] = _timestamp_from_utc(u_j_s['time_info'][1])
                    elif u_j_s['stage'] == 'completed':
                        u_j_s['finish_time'] = _timestamp_from_utc(u_j_s['time_info'][1])
                    #get some info from the client groups
                    for clnt in c_groups:
                        if clnt['function_name'] in u_j_s['job_desc']:
                            u_j_s['app_id'] = clnt['app_id']
                            u_j_s['client_groups'] = clnt['client_groups']
                            u_j_s['module'] = clnt['module_name']
                            u_j_s['method'] = clnt['function_name']
                            break
                    #log("*******From ujs result directly*******:\n")
                    #log(pformat(u_j_s))

                if (u_j_s['stage'] == 'started' and u_j_s['status'] == 'running'):
                    delta = datetime.datetime.utcnow() - datetime.datetime.fromtimestamp(u_j_s['exec_start_time']/1000)
                    delta = delta - datetime.timedelta(microseconds=delta.microseconds)
                    u_j_s['running_time'] = str(delta) #delta.total_seconds()
                elif u_j_s['stage'] == 'complete' or u_j_s['job_state'] == 'completed':
                    delta = (datetime.datetime.fromtimestamp(u_j_s['finish_time']/1000) -
                            datetime.datetime.fromtimestamp(u_j_s['exec_start_time']/1000))
                    delta = delta - datetime.timedelta(microseconds=delta.microseconds)
                    u_j_s['run_time'] = str(delta) #delta.total_seconds()
                elif (u_j_s['stage'] == 'created'
                      and u_j_s['status'] not in ['done','running','canceled by user','error']
                      and job_error[j_idx] == {}):
                    delta = (datetime.datetime.utcnow() - datetime.datetime.fromtimestamp(
                                    u_j_s['creation_time']/1000))
                    delta = delta - datetime.timedelta(microseconds=delta.microseconds)
                    u_j_s['queued_time'] = str(delta) #delta.total_seconds()
                    u_j_s['status'] = 'queued'
                else:
                    u_j_s['status'] = 'not started'

                ujs_ret.append(u_j_s)

        #log("Final count={}".format(len(ujs_ret)))
        return ujs_ret

    def get_exec_stats_from_cat(self):
        """
        get_exec_stats_from_cat: Get stats on completed jobs
        return an array of the following structure (example with data):
        {
             u'app_id': u'describe_rnaseq_experiment',
             u'app_module_name': u'KBaseRNASeq',
             u'creation_time': 1456863947.568,
             u'exec_start_time': 1456863953.739,
             u'finish_time': 1456863955.138,
             u'func_module_name': u'KBaseRNASeq',
             u'func_name': u'SetupRNASeqAnalysis',
             u'git_commit_hash': u'5de844e7303a8a30a94d4ca40f2b341439b8bb3c',
             u'is_error': True,
             u'user_id': u'srividya22'
        }
        """
        # Pull the data
        log("Fetching the data from Catalog API...")
        raw_stats = self.cat_client.get_exec_raw_stats({})
        #raw_stats = self.cat_client.get_exec_raw_stats({},{'begin': 1510558000, 'end': 1510680000})

        # Calculate queued_time and run_time (in seconds)
        for elem in raw_stats:
            tc = elem['creation_time']
            ts = elem['exec_start_time']
            tf = elem['finish_time']
            elem['queued_time'] = ts - tc
            elem['run_time'] = tf - ts

        log(pformat(raw_stats[0]))

        return raw_stats


    def get_client_groups_from_cat(self):
        """
        get_client_groups_from_cat
        return an array of the following structure (example with data):
        {
            u'app_id': u'assemblyrast/run_arast',
            u'client_groups': [u'bigmemlong'],
            u'function_name': u'run_arast',
            u'module_name': u'AssemblyRAST'},
        }
        """
        # Pull the data
        log("Fetching the client_groups data from Catalog API...")
        client_groups = self.cat_client.get_client_groups({})

        #log(pformat(client_groups[0]))

        return client_groups


    def get_exec_aggrTable_from_cat(self):
        """
        get_exec_stats_from_cat: Get stats on completed jobs
        return an array of the following structure (example with data):
        {
             u'app': u'kb_uploadmethods/import_sra_as_reads_from_web',
             u'func': u'import_sra_from_web',
             u'func_mod': u'kb_uploadmethods',
             u'n': 5,
             u'user': u'umaganapathyswork'
        }
        """
        # Pull the data
        log("Fetching the data from Catalog API...")
        aggr_tab = self.cat_client.get_exec_aggr_table({})

        log(pformat(aggr_tab[0]))

        return aggr_tab


    def get_exec_aggrStats_from_cat(self):
        """
        get_exec_aggr_from_cat: Get stats on aggregated execution results of KBase apps
        return an array of the following structure (example with data):
        {
             u'full_app_id': u'KBaseRNASeq/describe_rnaseq_experiment',
             u'module_name': u'KBaseRNASeq',
             u'number_of_calls': 689,
             u'number_of_errors': 117,
             u'time_range': u'*',
             u'total_exec_time': 10.807103612158034,
             u'total_queue_time': 127.90380222181479,
             u'type': u'a'
        }
        """
        # Pull the data
        aggr_stats = self.cat_client.get_exec_aggr_stats({})

        # Convert time from seconds to hours
        for kb_mod in aggr_stats:
            te = kb_mod['total_exec_time']
            tq = kb_mod['total_queue_time']
            kb_mod['total_exec_time'] = te/3600
            kb_mod['total_queue_time'] = tq/3600

        log(pformat(aggr_stats[0]))

        return aggr_stats


    def get_module_stats_from_cat(self):
        """
        get_module_stats_from_cat: Get stats on Modules
        """
        now = time.time()
        kb_modules = dict()
        for kb_module in self.cat_client.list_basic_module_info({'include_unreleased':True}):
            name = kb_module['module_name']
            v = self.cat_client.get_module_info({'module_name':name})['beta']
            vers = self.cat_client.list_released_module_versions({'module_name':name})
            s = 'b'
            if len(vers)>0:
                v = vers[0]
                s = 'r'
            if v is None:
                continue
            ct = len(v['narrative_methods'])
            days = (v['timestamp']/1000)/3600/24
            #print '%-40s %3d %3d' %(kb_module['module_name'],days,ct)
            kb_modules['%s:%d:%s' %(name,ct,s)] = days
        #log(pformat(kb_modules))

        # Generate time based summaries
        sorted_x = sorted(kb_modules, key=lambda i: int(kb_modules[i]))
        mods = dict()
        apps = dict()
        rmods = dict()
        rapps = dict()
        for bucket in range(184,300):
            mods[bucket] = 0
            apps[bucket] = 0
            rmods[bucket] = 0
            rapps[bucket] = 0
        for m in sorted_x:
            (name,ct,s) = m.split(':')
            d = kb_modules[m]
            bucket = int(d/91.25)
            if bucket not in mods:
                mods[bucket] = 0
                apps[bucket] = 0
                rmods[bucket] = 0
                rapps[bucket] = 0
            mods[bucket] += 1
            apps[bucket] += int(ct)
            if s == 'r':
                rmods[bucket] += 1
                rapps[bucket] += int(ct)
            #print '%-40s %3d %3d' %(name,int(ct),kb_modules[m])

        # Modules by Quarter
        tmods = 0
        tapps = 0
        trmods = 0
        trapps = 0
        Q = 1
        Y =16
        labels = dict()
        bucket = 184
        for year in range(16,21):
            for quarter in range(1,5):
                labels[bucket] = 'Q%d-%2d' % (quarter,year)
                bucket += 1
        for b in range(184,191):
            tmods += mods[b]
            tapps += apps[b]
            trmods += rmods[b]
            trapps += rapps[b]
            print '%5s %3d %3d       %3d %3d   %3d %3d' %(labels[b],tmods, tapps,trmods,trapps,tmods-trmods,tapps-trapps)

        return kb_modules


    def generate_app_report(self, metrics_dir, data_info, params):
        # create report
        report_text = 'Summary of app metrics for {}:\n\n'.format(params['ws_ids'])

        report_info = self.kbr.create_extended_report({
                        'message': report_text,
                        'report_object_name': 'kb_Metrics_report_' + str(uuid.uuid4()),
                        'workspace_name': params[self.PARAM_IN_WS]
                      })

        return report_info


    def generate_report(self, metrics_dir, data_info, params, col_caps=None):
        if col_caps is None:
            output_html_files = self._generate_html_report(metrics_dir, data_info)
        else:
            output_html_files = self._generate_html_report(metrics_dir, data_info, col_caps)

        output_json_files = self._generate_output_file_list(metrics_dir)

        # create report
        report_text = 'Summary of {} stats:\n\n'.format(params['stats_name'])

        report_info = self.kbr.create_extended_report({
                        'message': report_text,
                        'report_object_name': 'kb_Metrics_report_' + str(uuid.uuid4()),
                        'file_links': output_json_files,
                        'direct_html_link_index': 0,
                        'html_links': output_html_files,
                        'html_window_height': 366,
                        'workspace_name': params[self.PARAM_IN_WS]
                      })

        return report_info


    def _download_file_by_url(self, file_url):
        download_to_dir = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(download_to_dir)
        download_file = os.path.join(download_to_dir, 'genome_file.gbff.gz')

        try:
            urllib.urlretrieve(file_url, download_file)
        except HTTPError as e:
            log('The server couldn\'t download {}.'.format(file_url))
            log('Error code: ', e.code)
        except URLError as e:
            log('We failed to reach the server to download {}.'.format(file_url))
            log('Reason: ', e.reason)
        except IOError as e:
            log('Caught IOError when downloading {}.'.format(file_url))
            log('Error number: ', e.errno)
            log('Error code: ', e.strerror)
            if e.errno == errno.ENOSPC:
                log('No space left on device.')
            elif e.errno == 110:#[Errno ftp error] [Errno 110] Connection timed out
                log('Connection timed out, trying to urlopen!')
                try:
                    fh = urllib2.urlopen(file_url)
                    data = fh.read()
                    with open(download_file, "w") as dfh:
                        dfh.write(data)
                except:
                    log('Connection timed out, urlopen try also failed!')
                else:
                    pass
        else:# everything is fine
            pass

        return download_file


    def _get_file_content_by_url(self, file_url):
        req = Request(file_url)
        resp = ''
        try:
            resp = urlopen(req)
        except HTTPError as e:
            log('The server couldn\'t fulfill the request to download {}.'.format(file_url))
            log('Error code: ', e.code)
        except URLError as e:
            log('We failed to reach a server to download {}.'.format(file_url))
            log('Reason: ', e.reason)
        else:# everything is fine
            pass

        return resp


    def _generate_output_file_list(self, out_dir):
        """
        _generate_output_file_list: zip result files and generate file_links for report
        """
        log('start packing result files')

        output_files = list()

        output_directory = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(output_directory)
        output_file_path = os.path.join(output_directory, 'output_files.zip')
        self.zip_folder(out_dir, output_file_path)

        output_files.append({'path': output_file_path,
                             'name': os.path.basename(output_file_path),
                             'label': os.path.basename(output_file_path),
                             'description': 'Output files generated by kb_Metrics'})

        return output_files


    def zip_folder(self, folder_path, output_path):
        """Zip the contents of an entire folder (with that folder included in the archive).
        Empty subfolders could be included in the archive as well if the commented portion is used.
        """
        with zipfile.ZipFile(output_path, 'w',
                             zipfile.ZIP_DEFLATED,
                             allowZip64=True) as ziph:
            for root, folders, files in os.walk(folder_path):
                # Include all subfolders, including empty ones.
                #for folder_name in folders:
                #    absolute_path = os.path.join(root, folder_name)
                #    relative_path = os.path.join(os.path.basename(root), folder_name)
                #    print "Adding {} to archive.".format(absolute_path)
                #    ziph.write(absolute_path, relative_path)
                for f in files:
                    absolute_path = os.path.join(root, f)
                    relative_path = os.path.join(os.path.basename(root), f)
                    #print "Adding {} to archive.".format(absolute_path)
                    ziph.write(absolute_path, relative_path)

        print "{} created successfully.".format(output_path)

    def _write_headContent(self):
        """_write_headConten: returns the very first portion of the html file
        """
        head_content = ("\n<html>\n<head>\n"
            "<script type='text/javascript' src='https://www.google.com/jsapi'></script>\n"
            "<script type='text/javascript'>\n"
            "// Load the Visualization API and the controls package.\n"
            "  google.load('visualization', '1', {packages:['controls'], callback: drawDashboard});\n"
            "  google.setOnLoadCallback(drawDashboard);\n")

        return head_content

    def _write_callback_function(self, input_dt, col_caps=None):
        """
        _write_callback_function: write the callback function according to the input_dt and column captions
        """
        callback_func = ("\nfunction drawDashboard() {\n"
            "var data = new google.visualization.DataTable();\n")

        #table column captions
        if col_caps is None:
            col_caps = input_dt[0].keys()

        cols = []
        for i, col in enumerate(col_caps):
            for k in input_dt[0].keys():
                if col == k:
                    col_type = type(input_dt[0][k]).__name__
                    if (col_type == 'str' or col_type == 'unicode'):
                        col_type = 'string'
                    elif col_type == 'bool':
                        col_type = 'boolean'
                    else:
                        col_type = 'number'
                    callback_func += "data.addColumn('" + col_type + "','" + k + "');\n"
                    cols.append( col )

        #data rows
        dt_rows = ""
        for dt in input_dt:
            if dt_rows != "":
                dt_rows += ",\n"
            d_rows = []
            for j, c in enumerate( cols ):
                d_type = type(dt[c]).__name__
                if (d_type == 'str' or d_type == 'unicode'):
                    if dt[c] is None:
                        d_rows.append('"None"')
                    else:
                        d_rows.append('"' + dt[c] + '"')
                elif d_type == 'bool':
                    if dt[c]:
                        d_rows.append('true')
                    else:
                        d_rows.append('false')
                else:
                    if dt[c] is None:
                        d_rows.append('"None"')
                    else:
                        d_rows.append(str(dt[c]))

            dt_rows += '[' + ','.join(d_rows) + ']'

        callback_func += "\ndata.addRows([\n"
        callback_func += dt_rows
        callback_func += "\n]);"

        return callback_func


    def _write_charts(self):
        cat_picker = ("\nvar categoryPicker = new google.visualization.ControlWrapper({\n"
                "controlType: 'CategoryFilter',\n"
                "containerId: 'cat_picker_div',\n"
                "options: {\n"
                "//filterColumnIndex: 0, // filter by this column\n"
                "filterColumnLabel: 'user_id',\n"
                "ui: {\n"
                "    caption: 'Choose a value',\n"
                "    sortValues: true,\n"
                "    allowNone: true,\n"
                "    allowMultiple: true,\n"
                "    allowTyping: true\n"
                "  }\n"
                "},\n"
                "// Define an initial state, i.e. a set of metrics to be initially selected.\n"
                "//state: {'selectedValues': ['KBaseRNASeq', 'MEGAHIT', 'fba_tools']}\n"
                "state: {'selectedValues': ['qzhang', 'srividya22']}\n"
            "});\n")

        time_slider = ("\n//Create a range slider, passing some options\n"
            "var timeRangeSlider = new google.visualization.ControlWrapper({\n"
                "'controlType': 'NumberRangeFilter',\n"
                "'containerId': 'number_filter_div',\n"
                "'options': {\n"
                "'filterColumnLabel': 'run_time',\n"
                "'minValue': 0,\n"
                "'maxValue': 3600\n"
                "},\n"
                "'state': {'lowValue': 5, 'highValue': 600}\n"
                "});\n")

        num_slider1 = ("\n//Create a range slider, passing some options\n"
            "var numRangeSlider = new google.visualization.ControlWrapper({\n"
                "'controlType': 'NumberRangeFilter',\n"
                "'containerId': 'number_filter_div1',\n"
                "'options': {\n"
                "'filterColumnLabel': 'run_time',\n"
                "'minValue': 0,\n"
                "'maxValue': 3600\n"
                "},\n"
                "'state': {'lowValue': 5, 'highValue': 600}\n"
                "});\n")

        line_chart = ("var lineChart = new google.visualization.ChartWrapper({\n"
                "'chartType' : 'Line',\n"
                "'containerId' : 'line_div',\n"
                "'options': {\n"
                "'width': 600,\n"
                "'height': 300,\n"
                "'hAxis': {\n"
                "'title': 'app id'\n"
                "},\n"
                "'vAxis': {\n"
                "'title': 'Seconds'\n"
                "},\n"
                "'chartArea': {'left': 15, 'top': 25, 'right': 0, 'bottom': 15}\n"
                "},\n"
                "'view': {'columns': [6, 9, 10]}\n"
                "});\n")

        num_slider2 = ("\n//Create a range slider, passing some options\n"
                "var callsRangeSlider = new google.visualization.ControlWrapper({\n"
                "'controlType': 'NumberRangeFilter',\n"
                "'containerId': 'number_filter_div2',\n"
                "'options': {\n"
                "'filterColumnLabel': 'queued_time',\n"
                "'minValue': 1,\n"
                "'maxValue': 20000\n"
                "},\n"
                "'state': {'lowValue': 1000, 'highValue': 10000}\n"
                "});\n")

        pie_chart = ("\n//Create a pie chart, passing some options\n"
                "var pieChart = new google.visualization.ChartWrapper({\n"
                "'chartType': 'PieChart',\n"
                "'containerId': 'chart_div',\n"
                "'options': {\n"
                "'width': 300,\n"
                "'height': 300,\n"
                "'pieSliceText': 'value', //'label',\n"
                "'legend': 'none',\n"
                "'is3D': true,\n"
                "'chartArea': {'left': 15, 'top': 25, 'right': 0, 'bottom': 15},\n"
                "'title': 'Set your chart title, e.g., Number of calls per module'\n"
                "},\n"
                "// The pie chart will use the columns 'module_name' and 'number_of_calls'\n"
                "// out of all the available ones.\n"
                "'view': {'columns': [6, 9]}\n"
                "});\n")

        tab_chart = ("\n//create a list of columns for the table chart\n"
            "var filterColumns = [{\n"
            "// this column aggregates all of the data into one column for use with the string filter\n"
            "type: 'string',\n"
            "calc: function (dt, row) {\n"
            "for (var i = 0, vals = [], cols = dt.getNumberOfColumns(); i < cols; i++) {\n"
            "    vals.push(dt.getFormattedValue(row, i));\n"
            "}\n"
            "return vals.join('\\n');\n"
            "}\n"
            "}];\n"
            "var tab_columns = [];\n"
            "for (var j = 0, dcols = data.getNumberOfColumns(); j < dcols; j++) {\n"
            "    filterColumns.push(j);\n"
            "    tab_columns.push(j+1);\n"
            "}\n"
            "var stringFilter = new google.visualization.ControlWrapper({\n"
            "    controlType: 'StringFilter',\n"
            "    containerId: 'string_filter_div',\n"
            "    options: {\n"
            "        filterColumnIndex: 0,\n"
            "        matchType: 'any',\n"
            "        caseSensitive: false,\n"
            "        ui: {\n"
            "            label: 'Search table:'\n"
            "           }\n"
            "    },\n"
            "    view: {\n"
            "               columns: filterColumns\n"
            "          }\n"
            "});\n"
            "var table = new google.visualization.ChartWrapper({\n"
            "    chartType: 'Table',\n"
            "    containerId: 'table_div',\n"
            "    options: {\n"
            "        showRowNumber: true,\n"
            "        page: 'enable',\n"
            "        pageSize: 20\n"
            "    },\n"
            "    view: {\n"
            "               columns: tab_columns\n"
            "          }\n"
            "});\n")

        return cat_picker + time_slider + line_chart + num_slider2 + pie_chart + tab_chart


    def _write_dashboard(self):
        """
        _write_dashboard: writes the dashboard layout and bind controls with charts
        """
        #the dashboard components (table, charts and filters)
        dash_components = self._write_charts()
        dashboard = ("\n"
            "var dashboard = new google.visualization.Dashboard(document.querySelector('#dashboard_div'));\n"
            "dashboard.bind([categoryPicker], [pieChart, lineChart],[table]);\n"
            "//dashboard.bind([callsRangeSlider], [pieChart]);\n"
            "dashboard.bind([timeRangeSlider], [pieChart, lineChart]);\n"
            "dashboard.bind([stringFilter], [table]);\n"
            "dashboard.draw(data);\n"
        "}\n")

        return dash_components + dashboard

    def _write_footcontent(self):
        report_title = "Report_title_here"
        footContent = "</script></head>\n<body>\n"
        footContent += "<h4>" + report_title + "</h4>\n"
        footContent += "  <div id='dashboard_div'>\n" \
                "<div id='cat_picker_div'></div>\n" \
                "<div id='number_filter_div'></div>\n" \
                "<div style='display: inline-block'>\n" \
                "<div id='number_filter_div1'></div>\n" \
                "<div id='chart_div'></div>\n" \
                "</div>\n" \
                "<div style='display: inline-block'>\n" \
                "<div id='number_filter_div2'></div>\n" \
                "<div id='line_div'></div>\n" \
                "</div>\n" \
                "<div id='string_filter_div'></div>\n" \
                "<div id='table_div'></div>\n" \
                "</div>\n</body>\n</html>"

        return footContent

    def _write_html(self, out_dir, input_dt, col_caps=None):
        log('\nInput json with {} data items\n'.format(len(input_dt)))
        dt = input_dt[0:200]#For the sake of testing, limit the rows for datatable

        headContent = self._write_headContent()

        if col_caps is None:
            callbackFunc = self._write_callback_function(dt)
        else:
            callbackFunc = self._write_callback_function(dt, col_caps)

        dashboard = self._write_dashboard()

        footContent = self._write_footcontent()

        html_str = headContent + callbackFunc + dashboard + footContent
        log(html_str)

        html_file_path = os.path.join(out_dir, 'report_charts.html')

        with open(html_file_path, 'w') as html_file:
                html_file.write(html_str)

        return {'html_file': html_str, 'html_path': html_file_path}


    def _generate_html_report(self, out_dir, dt_info, col_caps=None):
        """
        _generate_html_report: generate html report given the json data in feat_counts

        """
        #log('start generating html report')
        html_report = list()

        if col_caps is None:
            html_file_path = self._write_html(out_dir, dt_info)
        else:
            html_file_path = self._write_html(out_dir, dt_info, col_caps)

        rpt_title = 'Report with charts'

        #log(html_file_path['html_file'])
        html_report.append({'path': html_file_path['html_path'],
                            'name': rpt_title,
                            'label': rpt_title,
                            'description': 'The report with charts'
                        })

        return html_report



