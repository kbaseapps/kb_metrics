import json
from pprint import pprint, pformat
import copy
import datetime
from pymongo import MongoClient
from pymongo import ASCENDING
from pymongo import DESCENDING
from bson.son import SON


def _datetime_from_utc(date_utc_str):
    try:#for u'2017-08-27T17:29:37+0000'
        dt = datetime.datetime.strptime(date_utc_str,'%Y-%m-%dT%H:%M:%S+0000')
    except ValueError as v_er:#for ISO-formatted date & time, e.g., u'2015-02-15T22:31:47.763Z'
        dt = datetime.datetime.strptime(date_utc_str,'%Y-%m-%dT%H:%M:%S.%fZ')
    return dt

def _convert_to_datetime(dt):
    new_dt = dt
    if (not isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime)):
        if isinstance(dt, int):# miliseconds
            new_dt = datetime.datetime.utcfromtimestamp(dt / 1000)
	    #pprint(new_dt)
        else:
            new_dt = _datetime_from_utc(dt)
    return new_dt


class MongoMetricsDBI:

    # Collection Names

    _DB_VERSION='db_version' #single

    _USERSTATE='userstate'#userjobstate.userstate
    _JOBSTATE='jobstate'#userjobstate.jobstate

    _AUTH2USERS='users'#auth2.users

    _USERPROFILES='profiles'#user_profile_db.profiles

    _EXEC_APPS='exec_apps'#exec_engine.exec_apps
    _EXEC_LOGS='exec_logs'#exec_engine.exec_logs
    _EXEC_TASKS='exec_tasks'#exec_engine.exec_tasks
    _TASK_QUEUE='task_queue'#exec_engine.task_queue

    _WS_WORKSPACES='workspaces'#workspace.workspaces


    def __init__(self, mongo_host, mongo_dbs, mongo_user, mongo_psswd):
        self.mongo_clients = dict()
        self.metricsDBs = dict()
        for m_db in mongo_dbs:
	    # create the client and authenticate
	    self.mongo_clients[m_db] = MongoClient("mongodb://"+mongo_user+":"+mongo_psswd
						+"@"+mongo_host+"/"+m_db)
	    # grab a handle to the database
            self.metricsDBs[m_db] = self.mongo_clients[m_db][m_db]

    # functions to query the databases...
    def aggr_user_logins(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws":1,"numObj":1,"meta":1}},
	    {"$group":{"_id":{"user":"$owner","year":"$year","month":"$month"},"year_mon_user_logins":{"$sum":1}}},
	    {"$sort":{"_id.user":1,"_id.year":1, "_id.month":1}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
	# list(m_cursor) only gets the keys [u'ok', u'result'] 
	m_result = m_cursor['result']
	# while list(m_result) gets the list of results
        return list(m_result)

    def aggr_total_logins(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws":1,"numObj":1,"meta":1}},
	    {"$group":{"_id":{"user":"$owner","year":"$year","month":"$month"},"count_user_ws_logins":{"$sum":1}}},
	    {"$group":{"_id":{"year":"$_id.year","month":"$_id.month"},"year_mon_total_logins":{"$sum":"$count_user_ws_logins"}}},
	    {"$sort":{"_id.year":1, "_id.month":1}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
	# list(m_cursor) only gets the keys [u'ok', u'result'] 
	m_result = m_cursor['result']
	# while list(m_result) gets the list of results
        return list(m_result)

    def aggr_user_ws(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws":1,"numObj":1,"meta":1}},
	    {"$group":{"_id":{"user":"$owner","year":"$year","month":"$month"},"count_user_ws":{"$sum":1}}},
	    {"$sort":{"_id.user":1,"_id.year":1, "_id.month":1}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
	# list(m_cursor) only gets the keys [u'ok', u'result'] 
	m_result = m_cursor['result']
	# while list(m_result) gets the list of results
        return list(m_result)

    def aggr_user_narratives(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime},"meta":{"$exists":True,"$not":{"$size":0}}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws":1,"numObj":1,"meta":1}},
	    {"$group":{"_id":{"user":"$owner","year":"$year","month":"$month"},"count_user_narratives":{"$sum":1}}},
	    {"$sort":{"_id.user":1,"_id.year":1, "_id.month":1}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
	# list(m_cursor) only gets the keys [u'ok', u'result'] 
	m_result = m_cursor['result']
	# while list(m_result) gets the list of results
        return list(m_result)

    def aggr_user_numObjs(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws":1,"numObj":1,"meta":1}},
	    {"$group":{"_id":{"user":"$owner","year":"$year","month":"$month"},"count_user_numObjs":{"$sum":"$numObj"}}},
	    {"$sort":{"_id.user":1,"_id.year":1, "_id.month":1}}
	]

	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
	# list(m_cursor) only gets the keys [u'ok', u'result'] 
	m_result = m_cursor['result']
	# while list(m_result) gets the list of results
        return list(m_result)


    def list_ws_narratives(self):
	# Define the pipeline operations
	pipeline = [
	    {"$match":{"meta":{"$exists":True,"$not":{"$size":0}}}},
	    {"$project":{"owner":1,"ws":1,"name":1,"meta":1}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
	# list(m_cursor) only gets the keys [u'ok', u'result'] 
	m_result = m_cursor['result']
	# while list(m_result) gets the list of results
        return list(m_result)


    def list_exec_tasks(self, minTime, maxTime):
        filter = {}

        creationTimeFilter = {}
        if minTime is not None:
            creationTimeFilter['$gte'] = minTime
        if maxTime is not None:
            creationTimeFilter['$lte'] = maxTime
        if len(creationTimeFilter) > 0:
            filter['creation_time'] = creationTimeFilter

        projection = {
		'_id':0,
                'app_job_id':1,
                'awe_job_id':1,
                'ujs_job_id':1,
                'job_input':1,
                'job_output':1,
		'input_shock_id':1,
		'output_shock_id':1,
                'exec_start_time':1,#1449160731753L--miliseconds
                'creation_time':1,#1449160731753L
                'finish_time':1
        }
	# grab handle(s) to the database collections needed
        self.kbtasks = self.metricsDBs['exec_engine'][MongoMetricsDBI._EXEC_TASKS]

	'''
        # Make sure we have an index on user, created and updated
        self.kbtasks.ensure_index([
            ('app_job_id', ASCENDING),
            ('creation_time', ASCENDING)],
            unique=True, sparse=False)
	'''

        return list(self.kbtasks.find(
                        filter, projection,
                        sort=[['creation_time', ASCENDING]]))


    def list_exec_apps(self, minTime, maxTime):
        filter = {}

        creationTimeFilter = {}
        if minTime is not None:
            creationTimeFilter['$gte'] = minTime
        if maxTime is not None:
            creationTimeFilter['$lte'] = maxTime
        if len(creationTimeFilter) > 0:
            filter['creation_time'] = creationTimeFilter

        projection = {
		'_id':0,
                'app_job_id':1,
                'app_job_state':1,# 'completed', 'suspend', 'in-progress','queued'
                'app_state_data':1,
                'creation_time':1,#
                'modification_time':1
        }

	# grab handle(s) to the database collections needed
        self.kbapps = self.metricsDBs['exec_engine'][MongoMetricsDBI._EXEC_APPS]

	'''
        # Make sure we have an index on user, created and updated
        self.kbapps.ensure_index([
            ('app_job_id', ASCENDING),
            ('app_job_state', ASCENDING),
            ('creation_time', ASCENDING),
            ('modification_time', ASCENDING)],
            unique=True, sparse=False)
	'''

        return list(self.kbapps.find(
                        filter, projection,
                        sort=[['creation_time', ASCENDING]]))


    def aggr_user_details(self, userIds, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"user":{"$in":userIds,"$ne":"kbasetest"},"create":{"$gte":_convert_to_datetime(minTime),"$lte":_convert_to_datetime(maxTime)}}},
            {"$project":{"user_id":"$user","email_address":"$email","full_name":"$display","account_created":"$create","most_recent_login":"$login","roles":1}},
	    {"$sort":{"account_created":1}}
	]

	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbusers = self.metricsDBs['auth2'][MongoMetricsDBI._AUTH2USERS]
	u_cursor = self.kbusers.aggregate(pipeline)
	# list(u_cursor) only gets the keys [u'ok', u'result'] 
	u_result = u_cursor['result']
	# while list(u_result) gets the list of results
	return list(u_result)


    def list_user_details(self, userIds, minTime, maxTime):
        filter = {}

        userFilter = {}
        if (userIds is not None and len(userIds) > 0):
            userFilter['$in'] = userIds
	elif userIds == []:
	    userFilter['$ne'] = 'kbasetest'
        if len(userFilter) > 0:
            filter['user'] = userFilter

        createFilter = {}
        if minTime is not None:
            createFilter['$gte'] = _convert_to_datetime(minTime)
        if maxTime is not None:
            createFilter['$lte'] = _convert_to_datetime(maxTime)
        if len(createFilter) > 0:
            filter['create'] = createFilter
	# exclude root, admin etc.
        filter['lastrst'] = {'$exists': False}

        projection = {
		'_id':0,
                'user':1,
                'email':1,
		'display':1,#full name
                'roles':1,
                'create':1,#ISODate("2017-05-24T22:52:27.990Z")
                'login':1
        }

	# grab handle(s) to the database collections needed
        self.kbusers = self.metricsDBs['auth2'][MongoMetricsDBI._AUTH2USERS]

	'''
        # Make sure we have an index on user, created and updated
        self.kbusers.ensure_index([
            ('user', ASCENDING),
            ('create', ASCENDING),
            ('login', ASCENDING)],
            unique=True, sparse=False)
	'''
        return list(self.kbusers.find(
                        filter, projection))#,
                        #sort=[['create', ASCENDING]]))


    def list_ujs_results(self, userIds, minTime, maxTime):
        filter = {}

        userFilter = {}
        if (userIds is not None and len(userIds) > 0):
            userFilter['$in'] = userIds
	elif userIds == []:
	    userFilter['$ne'] = 'kbasetest'
        if len(userFilter) > 0:
            filter['user'] = userFilter

        createdFilter = {}
        if minTime is not None:
            createdFilter['$gte'] = _convert_to_datetime(minTime)
        if maxTime is not None:
            createdFilter['$lte'] = _convert_to_datetime(maxTime)
        if len(createdFilter) > 0:
            filter['created'] = createdFilter
	filter['desc'] = {'$exists': True}
	filter['status'] = {'$exists': True}

        projection = {
                'user':1,
                'created':1,#datetime.datetime(2015, 1, 9, 19, 36, 8, 561000)
                'started':1,
                'updated':1,
                'status':1,# e.g., "queued", "ws.18657.obj.1", "Initializing", "canceled by user", etc.
                'progtype':1,# e.g., "percent", "task"
                'authparam':1,# "DEFAULT" or workspace_id
                'authstrat':1,# "DEFAULT" or "kbaseworkspace"
                'complete':1,
		'shared':1,
                'desc':1,
                'error':1,
                'errormsg':1,
                'estcompl':1,# e.g.,x ISODate("9999-04-03T08:56:32Z")
                'maxprog':1,
                'meta':1,
                'prog':1,
                'results':1,
                'service':1# e.g., "bulkio" or "qzhang"
        }

	# grab handle(s) to the database collections needed
        self.userstate = self.metricsDBs['userjobstate'][MongoMetricsDBI._USERSTATE]
        self.jobstate = self.metricsDBs['userjobstate'][MongoMetricsDBI._JOBSTATE]

	'''
        # Make sure we have an index on user, created and updated
        self.userstate.ensure_index(('created', ASCENDING), sparse=False)
        self.jobstate.ensure_index([
            ('user', ASCENDING),
            ('created', ASCENDING),
            ('updated', ASCENDING)],
            unique=True, sparse=False)
	'''

        #return list(self.jobstate.find(filter, projection))
        return list(self.jobstate.find(
                        filter, projection#,
                        ))#sort=[['created', ASCENDING]]))

