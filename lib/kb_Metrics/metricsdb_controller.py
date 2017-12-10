import warnings
import threading
import time
import datetime
import copy
import os
import random
import re
import uuid
import codecs
from copy import deepcopy

from pprint import pprint, pformat
from urlparse import urlparse
from kb_Metrics.metricsDBs import MongoMetricsDBI

from Workspace.WorkspaceClient import Workspace as Workspace
from Catalog.CatalogClient import Catalog
from NarrativeJobService.NarrativeJobServiceClient import NarrativeJobService
from UserAndJobState.UserAndJobStateClient import UserAndJobState
from UserProfile.UserProfileClient import UserProfile

def _datetime_from_utc(date_utc_str):
    try:#for u'2017-08-27T17:29:37+0000'
        dt = datetime.datetime.strptime(date_utc_str,'%Y-%m-%dT%H:%M:%S+0000')
    except ValueError as v_er:#for ISO-formatted date & time, e.g., u'2015-02-15T22:31:47.763Z'
        dt = datetime.datetime.strptime(date_utc_str,'%Y-%m-%dT%H:%M:%S.%fZ')
    return dt

def _convert_to_datetime(dt):
    new_dt = dt
    if (not isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime)):
        if isinstance(dt, int):
            new_dt = datetime.datetime.utcfromtimestamp(dt / 1000)
        else:
            new_dt = _datetime_from_utc(dt)
    return new_dt


class MetricsMongoDBController:

    def __init__(self, config):
        pprint("initializing mdb......")
        #pprint(config)
        # first grab the admin list
        self.adminList = []
        if 'admin-users' in config:
            adm_ids = config['admin-users'].split(',')
            for a_id in adm_ids:
                if a_id.strip():
                    self.adminList.append(a_id.strip())
        if not self.adminList:  # pragma: no cover
            warnings.warn('no "admin-users" are set in config of MetricsMongoDBController.')

        # make sure the minimal mongo settings are in place
        if 'mongodb-host' not in config: # pragma: no cover
            raise ValueError('"mongodb-host" config variable must be defined to start a MetricsMongoDBController!')
        if 'mongodb-databases' not in config: # pragma: no cover
            raise ValueError('"mongodb-databases" config variable must be defined to start a MetricsMongoDBController!')
	self.mongodb_dbList = []
        if 'mongodb-databases' in config:
            db_ids = config['mongodb-databases'].split(',')
            for d_id in db_ids:
                if d_id.strip():
                    self.mongodb_dbList.append(d_id.strip())
        if not self.mongodb_dbList:  # pragma: no cover
            warnings.warn('no "mongodb-databases" are set in config of MetricsMongoDBController.')
        # give warnings if no mongo user information is set
        if 'mongodb-user' not in config: # pragma: no cover
            warnings.warn('"mongodb-user" is not set in config of MetricsMongoDBController.')
            config['mongodb-user']=''
            config['mongodb-pwd']=''
        if 'mongodb-pwd' not in config: # pragma: no cover
            warnings.warn('"mongodb-pwd" is not set in config of MetricsMongoDBController.')
            config['mongodb-pwd']=''
        # instantiate the mongo client
        self.metrics_dbi = MongoMetricsDBI(
                    config['mongodb-host'],
 		    self.mongodb_dbList,
                    config['mongodb-user'],
                    config['mongodb-pwd'])

	#initialize clients for accessing other services
	'''
        self.workspace_url = config['workspace-url']
        self.ws_client = Workspace(self.workspace_url, token=token)
        self.cat_client = Catalog('https://ci.kbase.us/services/catalog', auth_svc='https://ci.kbase.us/services/auth/', token=token)
        self.njs_client = NarrativeJobService('https://ci.kbase.us/services/njs_wrapper', auth_svc='https://ci.kbase.us/services/auth/', token=token)
        self.ujs_client = UserAndJobState('https://ci.kbase.us/services/userandjobstate', auth_svc='https://ci.kbase.us/services/auth/', token=token)
        self.uprf_client = UserProfile('https://ci.kbase.us/services/user_profile/rpc', auth_svc='https://ci.kbase.us/services/auth/', token=token)
	'''

    # functions to get the requested records...
    def get_user_job_states(self, requesting_user, params, token):
	'''
	To get the job's 'status', 'complete'=true/false, etc., we can do joining as follows
	--userjobstate.jobstate['_id']==exec_engine.exec_tasks['ujs_job_id']
	To join exec_engine.exec_apps with exec_engine.exec_tasks:
	--exec_apps['app_job_id']==exec_tasks['app_job_id']
	To join exec_engine.exec_apps with ujs.jobstate:
	--ObjectId(exec_apps.app_job_state['job_id'])==jobstate['_id']
	return a list of the following structure:
        [
	 {'app_id': u'kb_Metrics/refseq_genome_counts',
          'canceled': 0,
          'creation_time': 1510159439977,
          'error': 0,
          'exec_start_time': 1510159441720,
          'finish_time': 1510159449612,
          'finished': 1,
          'job_desc': u'Execution engine job for kb_Metrics.refseq_genome_counts',
          'job_id': u'5a03344fe4b088e4b0e0e370',
          'job_state': u'completed',
          'method': u'refseq_genome_counts',
          'module': u'kb_Metrics',
          'result': [{u'report_name': u'kb_Metrics_report_f97f0567-fee5-48ea-8fc5-1f5e361ee2bd',
                      u'report_ref': u'25735/121/1'}],
          'run_time': '0:00:08',
          'stage': u'complete',
          'status': u'done',
          'time_info': [1510159439977,
                        1510159449612,
                        None],
          'user_id': u'qzhang',
          'wsid': 25735},
	}
	'''
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
	    minTime = _convert_to_datetime(minTime)
        if 'before' in params:
            maxTime = params['before']
	    maxTime = _convert_to_datetime(maxTime)
        if 'user_ids' in params:
            user_ids = params['user_ids']

	# query dbs to get lists of tasks and jobs
        exec_tasks = self.metrics_dbi.list_exec_tasks(minTime, maxTime)
	#pprint(exec_tasks[0:2])
	pprint("\n******Found {} tasks".format(len(exec_tasks)))
        exec_apps = self.metrics_dbi.list_exec_apps(minTime, maxTime)
	#pprint(exec_apps[0:2])
	pprint("\n******Found {} apps".format(len(exec_apps)))
        ujs_jobs = self.metrics_dbi.list_ujs_jobs(user_ids, minTime, maxTime)
	pprint(ujs_jobs[0:2])
	pprint("\n******Found {} ujs jobs".format(len(ujs_jobs)))

	# combine/join the apps, tasks and jobs lists to get the final return data
	# 1) combine/join the apps and tasks to get the app_task_list
	app_task_list = [] #deepcopy(exec_tasks) 
	for a in exec_apps:
	    for t in exec_tasks:
		if (('app_job_id' in t and a['app_job_id'] == t['app_job_id']) or
		    ('ujs_job_id' in t and a['app_job_id'] == t['ujs_job_id'])):
		    ta = deepcopy(t)
		    ta['job_state'] = a['app_job_state']
		    #app_task_list['app_state_data'] = a['app_state_data']
		    ta['modification_time'] = a['modification_time']
		    app_task_list.append(ta)
	pprint("\n******Combined {} app_tasks".format(len(app_task_list)))

	# 2) combine/join app_task_list with ujs_jobs list to get the final return data
	ujs_ret = []
	for j in ujs_jobs:
	    for lat in app_task_list:
		if lat['ujs_job_id'] == j['_id']:
		    j_a_t = deepcopy(j)
		    # from app_task_list
		    #j_a_t['app_job_id'] = lat['app_job_id']
		    j_a_t['creation_time'] = lat['creation_time']
		    j_a_t['job_state'] = lat['job_state']
		    if lat['exec_start_time']:
			j_a_t['exec_start_time'] = lat['exec_start_time']
		    if lat['finish_time']:
			j_a_t['finish_time'] = lat['finish_time']
		    if lat['modification_time']:
			j_a_t['modification_time'] = lat['modification_time']
		    if lat['job_input']:
			    j_a_t['app_id'] = lat['job_input']['method']
			    j_a_t['job_input'] = lat['job_input']
		    if lat['job_output']:
		        j_a_t['job_output'] = lat['job_output']
		    if (lat['job_state'] == 'completed' or 
			lat['job_state'] == 'suspend'):
			j_a_t['run_time'] = lat['modification_time'] - lat['exec_start_time']
		    elif lat['job_state'] == 'in-progress': 
			j_a_t['running_time'] = lat['modification_time'] - lat['exec_start_time']
		    elif lat['job_state'] == 'queued': 
			j_a_t['time_in_queue'] = lat['modification_time'] - lat['creation_time']
		    j_a_t['time_info'] = [lat['creation_time'], lat['modification_time'], None]

		    ujs_ret.append(j_a_t)

        return {'user_job_states': ujs_ret}


    def get_ujs_jobs(self, requesting_user, params, token):
	'''
        jobs = {
                'user':1,
                'created':1,
                'started':1,
                'updated':1,
                'status':1,
                'progtype':1,
                'authparam':1,
                'authstrat':1,
                'complete':1,
                'desc':1,
                'error':1,
                'errormsg':1,
                'estcompl':1,
                'maxprog':1,
                'meta':1,
                'prog':1,
                'results':1,
                'service':1
        }
	'''
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
	    minTime = _convert_to_datetime(minTime)
        if 'before' in params:
            maxTime = params['before']
	    maxTime = _convert_to_datetime(maxTime)
        if 'user_ids' in params:
            user_ids = params['user_ids']

        db_ret = self.metrics_dbi.list_ujs_results(user_ids, minTime, maxTime)

        return {'user_jobs': db_ret}


    def get_exec_apps(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
	    minTime = _convert_to_datetime(minTime)
        if 'before' in params:
            maxTime = params['before']
	    maxTime = _convert_to_datetime(maxTime)
        if 'user_ids' in params:
            user_ids = params['user_ids']


        db_ret = self.metrics_dbi.list_exec_apps(minTime, maxTime)
        return {'user_apps': db_ret}


    def get_exec_tasks(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
	    minTime = _convert_to_datetime(minTime)
        if 'before' in params:
            maxTime = params['before']
	    maxTime = _convert_to_datetime(maxTime)
        if 'user_ids' in params:
            user_ids = params['user_ids']

        db_ret = self.metrics_dbi.list_exec_tasks(minTime, maxTime)
        return {'user_tasks': db_ret}


    def get_user_details(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
	    minTime = _convert_to_datetime(minTime)
        if 'before' in params:
            maxTime = params['before']
	    maxTime = _convert_to_datetime(maxTime)
        if 'user_ids' in params:
            user_ids = params['user_ids']

        db_ret = self.metrics_dbi.list_user_details(user_ids, minTime, maxTime)
        return {'user_details': db_ret}


    def get_ujs_jobs(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
	    minTime = _convert_to_datetime(minTime)
        if 'before' in params:
            maxTime = params['before']
	    maxTime = _convert_to_datetime(maxTime)
        if 'user_ids' in params:
            user_ids = params['user_ids']

        db_ret = self.metrics_dbi.list_ujs_results(user_ids, minTime, maxTime)
        return {'user_ujs_results': db_ret}


    def is_admin(self, username):
        if username in self.adminList:
            return True
        return False

