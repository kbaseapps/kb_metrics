import time
import datetime
import json
import os
import re
import copy
import uuid
import shutil
import sys
from pprint import pprint, pformat
from urllib2 import Request, urlopen
from urllib2 import URLError, HTTPError
import urllib
import errno

from Bio import Entrez, SeqIO
from numpy import median, mean, max

from Workspace.WorkspaceClient import Workspace as Workspace
from Catalog.CatalogClient import Catalog
from NarrativeJobService.NarrativeJobServiceClient import NarrativeJobService
from UserAndJobState.UserAndJobStateClient import UserAndJobState
from UserProfile.UserProfileClient import UserProfile


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

def _convert_to_datetime(dt):
    new_dt = dt
    if (not isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime)):
        if isinstance(dt, int):
            new_dt = datetime.datetime.utcfromtimestamp(dt / 1000)
        else:
            new_dt = _datetime_from_utc(dt)
    return new_dt


class UJS_CAT_NJS_DataUtils:

    def __init__(self, config, provenance):
        self.workspace_url = config['workspace-url']
        self.callback_url = os.environ['SDK_CALLBACK_URL']
        self.token = os.environ['KB_AUTH_TOKEN']
        self.provenance = provenance

        self.scratch = os.path.join(config['scratch'], str(uuid.uuid4()))
        _mkdir_p(self.scratch)

        self.metrics_dir = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(self.metrics_dir)


    def generate_user_metrics(self, input_params, token):
        """
        """
        self.init_clients(token)

        if input_parameters.get('filter_str', None) is None:
            user_filter = ''
        else:
            user_filter = input_parameters['filter_str']

        kb_users = self.get_user_names(user_filter)

        user_names = []
        real_names = []
        for u in kb_users:
            user_names.append(u['username'])
            real_names.append(u['realname'])

        kb_uprof = self.get_user_profiles(user_names)

        return {'user_metrics': kb_uprof}


    def get_user_profiles(self, user_ids):
        """
        get_user_profiles: given the user ids, get a list of UserProfile of structure as below:
        typedef structure {
                username username;
                realname realname;
                string thumbnail;
        } User;
        typedef structure {
                User user;
                UnspecifiedObject profile;
        } UserProfile;

        """
        log("Fetching profile info for {} users:\n{}".format('the' if user_ids else 'all', user_ids if user_ids else ''))
        user_prof = self.uprf_client.get_user_profile(user_ids)
        #log(pformat(user_prof))
        return user_prof


    def get_user_names(self, filter_str):
        """
        get_user_names: given a filter string, get a list of User of structure as below:
        typedef structure {
                username username;
                realname realname;
                string thumbnail;
        } User;
        """
        log("Fetching user name details for {} users\n{}".format(
                'the' if filter_str else 'all', 'with id containing ' + filter_str if filter_str else ''))
        user_names = self.uprf_client.filter_users({'filter': filter_str})
        log(pformat(user_names))
        return user_names


    def generate_app_metrics(self, input_params, token):
        """
        """
        self.init_clients(token)

        params = self.process_parameters(input_params)
        user_ids = params['user_ids']
        time_start = params['time_start']
        time_end = params['time_end']
        job_stage = params['job_stage']

        ws_owners, ws_ids = self.get_user_workspaces(user_ids, 0, 0)
        ujs_ret = self.get_user_and_job_states(ws_ids)
        #log("Before time_stage filter:{}".format(len(ujs_ret)))

        jt_filtered_ujs = self.filter_by_time_stage(ujs_ret, job_stage, time_start, time_end)
        #log("After time_stage filter:{}".format(len(jt_filtered_ujs)))

        #jt_filtered_ujs = self.group_by_user(jt_filtered_ujs, user_ids)
        return {'job_states':jt_filtered_ujs}


    def get_user_workspaces(self, user_ids, showDeleted=0, showOnlyDeleted=0):
        """
        get_user_workspaces: given the user ids, get a list of data structure as the example below:
        typedef tuple<ws_id id,
              ws_name workspace,
              username owner,
              timestamp moddate,
              int max_objid,
              permission user_permission,
              permission globalread,
              lock_status lockstat,
              usermeta metadata> workspace_info;

        ws_info = self.ws_client.list_workspace_info({'owners':user_ids,
                        'showDeleted': showDeleted,
                        'showOnlyDeleted': showOnlyDeleted,
                        'perm':'r',
                        'excludeGlobal': 1,
                        'after': '2017-04-03T08:56:32Z',
                        'before': '2017-11-03T08:56:32Z'
                })

        return a list of ws_owners and ws_ids
        """
        #log("Fetching workspace ids for {} users:\n{}".format('the' if user_ids else 'all', user_ids if user_ids else ''))
        #ws_info = self.ws_client.list_workspace_info({})
        ws_info = self.ws_client.list_workspace_info({'owners':user_ids,
                        'showDeleted': showDeleted,
                        'showOnlyDeleted': showOnlyDeleted,
                        'perm':'r',
                        'excludeGlobal': 1
                })

        #log(pformat(ws_info))
        ws_ids = [ws[0] for ws in ws_info]
        ws_owners = [ws[2] for ws in ws_info]

        return (ws_owners, ws_ids)

    def get_user_and_job_states(self, ws_ids):
        """
        get_user_and_job_states: Get the user and job info for the given workspaces
        """
        #log("Fetching the job data...for these workspaces:\n{}".format(pformat(ws_ids)))

        wsj_states = []
        clnt_groups = self.get_client_groups_from_cat()
        counter = 0
        while counter < len(ws_ids) // 10:
            j_states = []
            wid_slice = ws_ids[counter * 10 : (counter + 1) * 10]
            wsj_states += self.retrieve_user_job_states(wid_slice, clnt_groups)
            counter += 1

        wsj_states += self.retrieve_user_job_states(ws_ids[counter * 10: ], clnt_groups)
        #log(pformat(wsj_states[0]))

        return wsj_states


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
        #log("Fetching the ujs data for workspace(s) {}...".format(pformat(wid_p)))
        ret_ujs = []
        try:
            nar_jobs = self.ujs_client.list_jobs2({
                'filter': 'S',#all jobs are returned
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
                job_stages = [j[3] for j in nar_jobs]#One of 'created', 'started', 'complete', 'canceled' or 'error'
                job_status = [j[4] for j in nar_jobs]##[u'done','running','canceled by user','......',...]
                job_time_info = [j[5] for j in nar_jobs]#tuple<timestamp started, timestamp last_update,timestamp est_complete>[[u'2017-10-27T17:29:37+0000', u'2017-10-27T17:29:42+0000', None],...]
                job_progress_info = [j[6] for j in nar_jobs]#tuple<total_progress prog, max_progress max, progress_type ptype>
                job_complete = [j[7] for j in nar_jobs]#[1,1,...,0,..]
                job_error = [j[8] for j in nar_jobs]#[1,0,...,0,..]
                job_auth_info = [j[9] for j in nar_jobs]#[[u'kbaseworkspace', u'25735'],...]
                job_meta = [j[10] for j in nar_jobs]#[{u'cell_id': u'828d2e3c-5c5d-4c4c-9de8-4aacb875c074',u'run_id': u'a05df5b3-2d3e-4e4a-9a32-173acaa9bd0c',u'tag': u'beta',u'token_id': u'2dea84eb-8f40-4516-b18e-f284cc6bb107'},...]
                job_desc = [j[11] for j in nar_jobs]#[u'Execution engine job for kb_Metrics.count_ncbi_genome_features',...]
                job_res = [j[12] for j in nar_jobs]#[{},None,...]

                ret_ujs = self.retrieve_ujs_via_njs(c_groups, job_ids, job_owners,
                                job_stages, job_status, job_time_info, job_error, job_desc)

        return ret_ujs

    def retrieve_ujs_via_njs(self, c_groups, job_ids, job_owners, job_stages,
                        job_status, job_time_info,job_error, job_desc):
        ujs_ret = []
        try:
            #log("Calling njs.check_jobs for {} jobs".format(len(job_ids)))
            job_info = self.njs_client.check_jobs({
                        'job_ids': job_ids, 'with_job_params': 1
                })
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
                        elif (u_j_s['stage'] == 'completed' or u_j_s['stage'] == 'complete'):
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
                    elif (u_j_s['stage'] == 'completed' or u_j_s['stage'] == 'complete'):
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
                    u_j_s['status'] = 'not created'

                ujs_ret.append(u_j_s)

        #log("Job count={}".format(len(ujs_ret)))
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
        #log("Fetching the exec stats data from Catalog API...")
        raw_stats = self.cat_client.get_exec_raw_stats({})

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
        get_client_groups_from_cat: Get the client_groups data from Catalog API
        return an array of the following structure (example with data):
        {
            u'app_id': u'assemblyrast/run_arast',
            u'client_groups': [u'bigmemlong'],
            u'function_name': u'run_arast',
            u'module_name': u'AssemblyRAST'},
        }
        """
        # Pull the data
        client_groups = self.cat_client.get_client_groups({})

        #log("\nClient group example:\n{}".format(pformat(client_groups[0])))

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
        log("Fetching the exec_aggr table data from Catalog API...")
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
        log("Fetching the exec_aggr stats data from Catalog API...")
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
        # Pull the data
        log("Fetching the module stats data from Catalog API...")
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


    def group_by_user(self, job_sts, user_ids):
        grouped_ujs = []
        if user_ids == []:
            return {'user_id': 'all_users', 'job_states': job_sts}

        for uid in user_ids:
            ujs_by_user = []
            for ujs_i in job_sts:
                if uid == ujs_i['user_id']:
                    ujs_by_user.append(ujs_i)
            if len(ujs_by_user) > 0:
                grouped_ujs.append({'user_id': uid, 'job_states': ujs_by_user})

        return grouped_ujs


    def filter_by_time_stage(self, job_sts, j_stage, j_start_time, j_end_time):
        filtered_ujs = []
        for ujs_i in job_sts:
            if isinstance(ujs_i['creation_time'], int):
                cr_time = datetime.datetime.utcfromtimestamp(ujs_i['creation_time'] / 1000)
            else:
                cr_time = _datetime_from_utc(ujs_i['creation_time'])
            #log("Comparing {} between {} and {}".format(str(cr_time), str(j_start_time), str(j_end_time)))
            if (cr_time <= j_end_time and cr_time >= j_start_time):
                if (j_stage == 'all' or j_stage == ujs_i['stage']):
                    filtered_ujs.append(ujs_i)

        return filtered_ujs


    def init_clients(self, token):
        self.ws_client = Workspace(self.workspace_url, token=token)
        #self.cat_client = Catalog(self.callback_url)
        self.cat_client = Catalog('https://kbase.us/services/catalog', auth_svc='https://kbase.us/services/auth/', token=token)
        self.njs_client = NarrativeJobService('https://kbase.us/services/njs_wrapper', auth_svc='https://kbase.us/services/auth/', token=token)
        self.ujs_client = UserAndJobState('https://kbase.us/services/userandjobstate', auth_svc='https://kbase.us/services/auth/', token=token)
        self.uprf_client = UserProfile('https://kbase.us/services/user_profile/rpc', auth_svc='https://kbase.us/services/auth/', token=token)


    def process_parameters(self, params):
        if params.get('user_ids', None) is None:
            params['user_ids'] = []
        else:
            if not isinstance(params['user_ids'], list):
                raise ValueError('Variable user_ids' + ' must be a list.')

        if not params.get('time_range', None) is None:
            time_start, time_end = params['time_range']
            params['time_start'] = _convert_to_datetime(time_start)
            params['time_end'] = _convert_to_datetime(time_end)
        else: #set the most recent 48 hours range
            params['time_end'] = datetime.datetime.utcnow()
            params['time_start'] = params['time_end'] - datetime.timedelta(hours=48)

        if params.get('job_stage', None) is None:
            params['job_stage'] = 'all'
        if params['job_stage'] == 'completed':
            params['job_stage'] = 'complete'

        return params
