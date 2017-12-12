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
from bson.objectid import ObjectId

from pprint import pprint, pformat
from urlparse import urlparse
from kb_Metrics.metricsDBs import MongoMetricsDBI

from Workspace.WorkspaceClient import Workspace as Workspace
from Catalog.CatalogClient import Catalog
from NarrativeJobService.NarrativeJobServiceClient import NarrativeJobService
from UserAndJobState.UserAndJobStateClient import UserAndJobState
from UserProfile.UserProfileClient import UserProfile

def _timestamp_from_utc(date_utc_str):
    dt = _datetime_from_utc(date_utc_str)
    return int(time.mktime(dt.timetuple())) #in miliseconds

def _unix_time_millis_from_datetime(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds()*1000)

def _datetime_from_utc(date_utc_str):
    try:#for u'2017-08-27T17:29:37+0000'
        dt = datetime.datetime.strptime(date_utc_str,'%Y-%m-%dT%H:%M:%S+0000')
    except ValueError as v_er:#for ISO-formatted date & time, e.g., u'2015-02-15T22:31:47.763Z'
        dt = datetime.datetime.strptime(date_utc_str,'%Y-%m-%dT%H:%M:%S.%fZ')
    return dt


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

	params = self.process_parameters(params)

	# query dbs to get lists of tasks and jobs
        exec_tasks = self.metrics_dbi.list_exec_tasks(params['minTime'], params['maxTime'])
	#pprint(exec_tasks[0:2])
	pprint("\n******Found {} tasks".format(len(exec_tasks)))
        exec_apps = self.metrics_dbi.list_exec_apps(params['minTime'], params['maxTime'])
	#pprint(exec_apps[0:2])
	pprint("\n******Found {} apps".format(len(exec_apps)))
	ujs_jobs = self.metrics_dbi.list_ujs_results(params['user_ids'], params['minTime'], params['maxTime'])
	#pprint(ujs_jobs[0:2])
	pprint("\n******Found {} ujs jobs".format(len(ujs_jobs)))

	# combine/join the apps, tasks and jobs lists to get the final return data
	# 1) combine/join the apps and tasks to get the app_task_list
	app_task_list = [] 
	for t in exec_tasks:
	    ta = copy.deepcopy(t)
	    for a in exec_apps:
		if (('app_job_id' in t and a['app_job_id'] == t['app_job_id']) or
		    ('ujs_job_id' in t and a['app_job_id'] == t['ujs_job_id'])):
		    ta['job_state'] = a['app_job_state']
		    #app_task_list['app_state_data'] = a['app_state_data']
		    #ta['modification_time'] = a['modification_time']
	    app_task_list.append(ta)

	# 2) combine/join app_task_list with ujs_jobs list to get the final return data
	ujs_ret = []
	for j in ujs_jobs:
	    j_a_t = copy.deepcopy(j)
	    j_a_t['creation_time'] = _unix_time_millis_from_datetime(j['created'])
	    if 'started' in j:
		j_a_t['exec_start_time'] = _unix_time_millis_from_datetime(j['started'])
	    j_a_t['modification_time'] = _unix_time_millis_from_datetime(j['updated'])
	    est = j.get('estcompl', None)
	    estcompl = None if est is None else _unix_time_millis_from_datetime(est)
	    j_a_t['time_info'] = [j_a_t['creation_time'], j_a_t['modification_time'], estcompl]

	    if 'complete' in j and j['complete'] == True:
		if 'error' in j and j['error'] == False:
		    j_a_t['job_state'] = 'completed'
		elif 'error' in j and j['error'] == True: 
		    j_a_t['job_state'] = 'suspend' 
	    elif 'complete' in j and j['complete'] == False:
		if 'error' in j and j['error'] == False:
		    if j['status'] == "Initializing":
		        j_a_t['job_state'] = j['status']
		    elif 'started' in j: 
		        j_a_t['job_state'] = 'in-progress' 
	    elif j['created'] == j['updated']:
		j_a_t['job_state'] = 'not-started' 
	    elif j['created'] < j['updated'] and 'started' not in j:
		j_a_t['job_state'] = 'queued'

	    for lat in app_task_list:
		if ObjectId(lat['ujs_job_id']) == j['_id']:
		    if 'job_state' not in j_a_t:
			j_a_t['job_state'] = lat['job_state']
		if 'job_input' in lat:
		    j_a_t['job_input'] = lat['job_input']
		    if 'app_id' in lat['job_input']:
			j_a_t['app_id'] = lat['job_input']['app_id']
		    if 'method' in lat['job_input']:
			j_a_t['method'] = lat['job_input']['method']
		    if 'wsid' in lat['job_input']:
			j_a_t['wsid'] = lat['job_input']['wsid']
		    elif 'params' in lat['job_input']:
			if 'ws_id' in lat['job_input']['params']:
			    j_a_t['wsid'] = lat['job_input']['params']['ws_id']
	        if 'job_output' in lat:
		    j_a_t['job_output'] = lat['job_output']
 
	    if j_a_t['job_state'] == 'completed':
		j_a_t['finish_time'] = j_a_t['modification_time']
		j_a_t['run_time'] = j_a_t['finish_time'] - j_a_t['exec_start_time']
	    if j_a_t['job_state'] == 'suspend':
		j_a_t['run_time'] = j_a_t['modification_time'] - j_a_t['exec_start_time']
	    elif j_a_t['job_state'] == 'in-progress': 
		j_a_t['running_time'] = j_a_t['modification_time'] - j_a_t['exec_start_time']
	    elif lat['job_state'] == 'queued': 
		j_a_t['time_in_queue'] = j_a_t['modification_time'] - j_a_t['creation_time']

	    ujs_ret.append(j_a_t)

        return {'ujs_results': ujs_ret}


    def get_ujs_results(self, requesting_user, params, token):
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

	params = self.process_parameters(params)

        db_ret = self.metrics_dbi.list_ujs_results(params['user_ids'], params['minTime'], params['maxTime'])

        return {'ujs_results': db_ret}


    def get_exec_apps(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)

        db_ret = self.metrics_dbi.list_exec_apps(params['minTime'], params['maxTime'])

        return {'user_apps': db_ret}


    def get_exec_tasks(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)

        db_ret = self.metrics_dbi.list_exec_tasks(params['minTime'], params['maxTime'])

        return {'user_tasks': db_ret}


    def get_user_details(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)

        db_ret = self.metrics_dbi.list_user_details(params['user_ids'], params['minTime'], params['maxTime'])

        return {'user_details': db_ret}


    def process_parameters(self, params):
        if params.get('user_ids', None) is None:
            params['user_ids'] = []
        else:
            if not isinstance(params['user_ids'], list):
                raise ValueError('Variable user_ids' + ' must be a list.')
	if 'kbasetest' in params['user_ids']:
		params['user_ids'].remove('kbasetest')

        if not params.get('epoch_range', None) is None:
            minTime, maxTime = params['epoch_range']
            params['minTime'] = minTime
            params['maxTime'] = maxTime
        else: #set the most recent 48 hours range
            maxTime = datetime.datetime.utcnow()
            minTime = maxTime - datetime.timedelta(hours=48)
            params['minTime'] = _unix_time_millis_from_datetime(minTime)
            params['maxTime'] = _unix_time_millis_from_datetime(maxTime)
        return params


    def is_admin(self, username):
        if username in self.adminList:
            return True
        return False

