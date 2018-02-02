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
from kb_Metrics.metricsDBs import MongoMetricsDBI
from Catalog.CatalogClient import Catalog

class MetricsMongoDBController:

    def __init__(self, config):
        #pprint("initializing mdb......")
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
	# for access to the Catalog API
	self.auth_service_url = config['auth-service-url']
	self.catalog_url = config['kbase-endpoint'] + '/catalog'


    ## function(s) to update the metrics db
    def update_metrics(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to invoke this action.')
	#self.db.add_user_info(user_id, email_addr, creation_time, login_time, full_name,
	                        #roles, status='A', cancellation={}, kb_internal=False,time_stamp=None)
	db_ret = self.get_activities(requesting_user, params, token)
	act_list = db_ret['metrics_result']
	#pprint(act_list)
	try:
	    insert_ret = self.metrics_dbi.insert_activity_records(act_list)
	except BulkWriteError as bwe:
	    pprint(bwe.details)
	return db_ret


    ## functions to get the requested records...
    def get_activities(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)
	params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
	params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_daily_activities(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_total_logins(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)
	params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
	params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_total_logins(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_logins(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)
	params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
	params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_logins(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_ws(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)
	params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
	params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_ws(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_narratives(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)
	params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
	params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_narratives(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_numObjs(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)
	params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
	params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_numObjs(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

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
	params = self.process_parameters(params)
        if not self.is_admin(requesting_user):
            #raise ValueError('You do not have permission to view this data.')
            #pprint(requesting_user + ': You have permission to view ONLY your jobs.')
	    params['user_ids'] = [requesting_user]

	# query dbs to get lists of tasks and jobs
        exec_tasks = self.metrics_dbi.list_exec_tasks(params['minTime'], params['maxTime'])

        exec_apps = self.metrics_dbi.list_exec_apps(params['minTime'], params['maxTime'])

	ujs_jobs = self.metrics_dbi.list_ujs_results(params['user_ids'], params['minTime'], params['maxTime'])
	ujs_jobs = self.convert_isodate_to_millis(ujs_jobs, ['created', 'started', 'updated', 'estcompl'])
	#pprint("\n******Found {} ujs jobs".format(len(ujs_jobs)))

        clnt_groups = self.get_client_groups_from_cat(token)
 
        return {'job_states': self.join_app_task_ujs(exec_tasks, exec_apps, ujs_jobs, clnt_groups)}


    def join_app_task_ujs(self, exec_tasks, exec_apps, ujs_jobs, c_groups):
	"""
	combine/join the apps, tasks and jobs lists to get the final return data
	"""
	# 0) get the ws_narrative data for lookups
	ws_narratives = self.metrics_dbi.list_ws_narratives()
	# 1) combine/join the apps and tasks to get the app_task_list
	app_task_list = []
	for t in exec_tasks:
	    ta = copy.deepcopy(t)
	    for a in exec_apps:
		if (('app_job_id' in t and a['app_job_id'] == t['app_job_id']) or
		    ('ujs_job_id' in t and a['app_job_id'] == t['ujs_job_id'])):
		    ta['job_state'] = a['app_job_state']
	    app_task_list.append(ta)

	# 2) combine/join app_task_list with ujs_jobs list to get the final return data
	ujs_ret = []
	for j in ujs_jobs:
	    u_j_s = copy.deepcopy(j)
	    u_j_s['job_id'] = str(u_j_s['_id'])
 	    del u_j_s['_id']	    
	    u_j_s['creation_time'] = j['created']
	    if 'started' in j:
		u_j_s['exec_start_time'] = j['started']
	    u_j_s['modification_time'] = j['updated']
	    u_j_s['estcompl'] = j.get('estcompl', None)
	    u_j_s['time_info'] = [u_j_s['creation_time'], u_j_s['modification_time'], u_j_s['estcompl']]
	    if not u_j_s.get('authstrat', None) is None:
		if u_j_s.get('authstrat', None) == 'kbaseworkspace':
		    u_j_s['wsid'] = u_j_s['authparam']
	    if not u_j_s.get('desc', None) is None:
		desc = u_j_s['desc'].split()[-1]
		if '.' in desc:
		    u_j_s['method'] = desc

	    # Assuming complete, error and status all exist in the records returned
	    if j['complete'] == True:
		if j['error'] == False:
		    u_j_s['job_state'] = 'completed'
		else: 
		    u_j_s['job_state'] = 'suspend' 
	    else:
		if j['error'] == False:
		    if j['status'] == "Initializing" or j['status'] == 'queued':
		        u_j_s['job_state'] = j['status']
		    elif 'canceled' in j['status'] or 'cancelled' in j['status']: 
		        u_j_s['job_state'] = 'canceled' 
		    elif 'started' in j: 
		        u_j_s['job_state'] = 'in-progress' 
		    elif j['created'] == j['updated']:
			u_j_s['job_state'] = 'not-started' 
		    elif j['created'] < j['updated'] and 'started' not in j:
			u_j_s['job_state'] = 'queued'
		    else:
			u_j_s['job_state'] = 'unknown'

	    for lat in app_task_list:
		if ObjectId(lat['ujs_job_id']) == j['_id']:
		    if 'job_state' not in u_j_s:
			u_j_s['job_state'] = lat['job_state']

		    if 'job_input' in lat:
			u_j_s['job_input'] = lat['job_input']
			if u_j_s.get('app_id', None) is None:
			    u_j_s['app_id'] = self.parse_app_id(lat)

			if u_j_s.get('method', None) is None:
			    u_j_s['method'] = self.parse_method(lat)

			if u_j_s.get('wsid', None) is None:
			    if 'wsid' in lat['job_input']:
				u_j_s['wsid'] = lat['job_input']['wsid']
			    elif 'params' in lat['job_input']:
				if 'ws_id' in lat['job_input']['params']:
				    u_j_s['wsid'] = lat['job_input']['params']['ws_id']
				if 'workspace' in lat['job_input']['params']:
				    u_j_s['workspace_name'] = lat['job_input']['params']['workspace']
				elif 'workspace_name' in lat['job_input']['params']:
				    u_j_s['workspace_name'] = lat['job_input']['params']['workspace_name']

		    if 'job_output' in lat:
			u_j_s['job_output'] = lat['job_output']

	    if (u_j_s.get('app_id', None) is None and
		not u_j_s.get('method', None) is None):
		    u_j_s['app_id'] = u_j_s['method'].replace('.', '/')

	    #get the narrative name and version if any
	    if not u_j_s.get('wsid', None) is None:
		n_nm, n_obj = self.map_narrative(u_j_s['wsid'], ws_narratives) 
		if n_nm != "" and n_obj != 0:
		    u_j_s['narrative_name'] = n_nm
		    u_j_s['narrative_objNo'] = n_obj  

	    #get some info from the client groups
	    for clnt in c_groups:
		if 'app_id' in u_j_s and clnt['module_name'] == u_j_s['app_id']:
		    #pprint("client func={} and ujs func={}".format(clnt['function_name'],u_j_s['method']))
		    u_j_s['client_groups'] = clnt['client_groups']
		    #u_j_s['module'] = clnt['module_name']
		    #u_j_s['function'] = clnt['function_name']
		    break
	    #default client groups to 'njs'
	    if u_j_s.get('client_groups', None) is None:
		u_j_s['client_groups'] = ['njs']

	    #set the run/running/in_queue time
	    if u_j_s['job_state'] == 'completed':
		u_j_s['finish_time'] = u_j_s['modification_time']
		u_j_s['run_time'] = u_j_s['finish_time'] - u_j_s['exec_start_time']
	    if u_j_s['job_state'] == 'suspend':
		u_j_s['finish_time'] = u_j_s['modification_time']
		u_j_s['run_time'] = u_j_s['modification_time'] - u_j_s['exec_start_time']
	    elif u_j_s['job_state'] == 'in-progress': 
		u_j_s['running_time'] = u_j_s['modification_time'] - u_j_s['exec_start_time']
	    elif u_j_s['job_state'] == 'queued': 
		u_j_s['time_in_queue'] = u_j_s['modification_time'] - u_j_s['creation_time']

	    ujs_ret.append(u_j_s)
	return ujs_ret

    def parse_app_id(self, lat):
	app_id = ''
	if 'app_id' in lat['job_input']:
	    if '/' in lat['job_input']['app_id']:
		app_id = lat['job_input']['app_id']
	    elif '.' in lat['job_input']['app_id']:
		app_id = lat['job_input']['app_id'].replace('.', '/')
	    else:
		app_id = lat['job_input']['app_id']

	return app_id

    def parse_method(self, lat):
	method = ''
	if 'method' in lat['job_input']:
	    if '.' in lat['job_input']['method']:
		method = lat['job_input']['method']
	    elif '/' in lat['job_input']['method']:
		method = lat['job_input']['method'].replace('/', '.')
	    else:
		method = lat['job_input']['method']

	return method_id

    def map_narrative(self, ws_id, ws_narratives):
	"""
	get the narrative name and version
	"""
	n_name = ''
	n_obj = 0
	ws_name = ''
	ws_owner = ''
	for ws in ws_narratives:
	    if str(ws['ws']) == str(ws_id):
		ws_name = ws['name']
		ws_owner = ws['owner']
		n_name = ws_name
		if not ws.get('meta', None) is None:
		    w_meta = ws['meta']
		    for w_m in w_meta:
			if w_m['k'] == 'narrative':
			    n_obj = w_m['v']
			elif w_m['k'] == 'narrative_nice_name':
			    n_name = w_m['v']
			else:
			    pass
		break
	return (n_name, n_obj)

    def get_ujs_results(self, requesting_user, params, token):
	params = self.process_parameters(params)
        if not self.is_admin(requesting_user):
            #pprint(requesting_user + ': You have permission to view ONLY your jobs.')
	    params['user_ids'] = [requesting_user]
	
	db_ret = self.metrics_dbi.list_ujs_results(params['user_ids'], params['minTime'], params['maxTime'])
	for dr in db_ret:
	    dr['_id'] = str(dr['_id'])
	db_ret = self.convert_isodate_to_millis(db_ret, ['created', 'started', 'updated', 'estcompl'])
	
        return {'metrics_result': db_ret}

    def get_exec_apps(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)

        db_ret = self.metrics_dbi.list_exec_apps(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_exec_tasks(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)

        db_ret = self.metrics_dbi.list_exec_tasks(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_details(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

	params = self.process_parameters(params)

        #db_ret = self.metrics_dbi.list_user_details(params['user_ids'], params['minTime'], params['maxTime'])
        db_ret = self.metrics_dbi.aggr_user_details(params['user_ids'], params['minTime'], params['maxTime'])
	if len(db_ret) == 0:
	    pprint("No records returned!")
	else:
	    db_ret = self.convert_isodate_to_millis(db_ret, ['account_created', 'most_recent_login'])
        return {'metrics_result': db_ret}

    def process_parameters(self, params):
        if params.get('user_ids', None) is None:
            params['user_ids'] = []
        else:
            if not isinstance(params['user_ids'], list):
                raise ValueError('Variable user_ids' + ' must be a list.')
	if 'kbasetest' in params['user_ids']:
		params['user_ids'].remove('kbasetest')

        if not params.get('epoch_range', None) is None:
            start_time, end_time = params['epoch_range']
	    if (not start_time is None and not end_time is None):
		start_time = _convert_to_datetime(start_time)
		end_time = _convert_to_datetime(end_time)
		params['minTime'] = _unix_time_millis_from_datetime(start_time)
		params['maxTime'] = _unix_time_millis_from_datetime(end_time)
	    elif (not start_time is None and end_time is None):
		start_time = _convert_to_datetime(start_time)
		end_time = start_time + datetime.timedelta(hours=48)
		params['minTime'] = _unix_time_millis_from_datetime(start_time)
		params['maxTime'] = _unix_time_millis_from_datetime(end_time)
	    elif (start_time is None and not end_time is None):
		end_time = _convert_to_datetime(end_time)
		start_time = end_time - datetime.timedelta(hours=48)
		params['minTime'] = _unix_time_millis_from_datetime(start_time)
		params['maxTime'] = _unix_time_millis_from_datetime(end_time)
        else: #set the most recent 48 hours range
            end_time = datetime.datetime.utcnow()
            start_time = end_time - datetime.timedelta(hours=48)
            params['minTime'] = _unix_time_millis_from_datetime(start_time)
            params['maxTime'] = _unix_time_millis_from_datetime(end_time)
        return params


    def get_client_groups_from_cat(self, token):
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
	#initialize client(s) for accessing other services
        self.cat_client = Catalog(self.catalog_url,
				auth_svc=self.auth_service_url, token=token)
        # Pull the data
        client_groups = self.cat_client.get_client_groups({})
        #log("\nClient group example:\n{}".format(pformat(client_groups[0])))

        return client_groups

    def is_admin(self, username):
        if username in self.adminList:
            return True
        return False

    def convert_isodate_to_millis(self, src_list, dt_list):
	for dr in src_list:
	    for dt in dt_list:
		if (dt in dr and isinstance(dr[dt], datetime.datetime)):
        	    dr[dt] = _unix_time_millis_from_datetime(dr[dt])#dr[dt].__str__()
	return src_list


def _timestamp_from_utc(date_utc_str):
    dt = _datetime_from_utc(date_utc_str)
    return int(time.mktime(dt.timetuple())) #in miliseconds

def _datetime_from_utc(date_utc_str):
    try:#for u'2017-08-27T17:29:37+0000'
        dt = datetime.datetime.strptime(date_utc_str,'%Y-%m-%dT%H:%M:%S+0000')
    except ValueError as v_er:#for ISO-formatted date & time, e.g., u'2015-02-15T22:31:47.763Z'
        dt = datetime.datetime.strptime(date_utc_str,'%Y-%m-%dT%H:%M:%S.%fZ')
    return dt

def _unix_time_millis_from_datetime(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds()*1000)

def _convert_to_datetime(dt):
    new_dt = dt
    if (not isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime)):
        if isinstance(dt, int):
            new_dt = datetime.datetime.utcfromtimestamp(dt / 1000)
        else:
            new_dt = _datetime_from_utc(dt)
    return new_dt

