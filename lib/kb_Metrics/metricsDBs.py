import json
from pprint import pprint, pformat
import copy
import datetime
from pymongo import MongoClient
from pymongo import ASCENDING
from pymongo import DESCENDING


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
                        filter, projection,
                        sort=[['create', ASCENDING]]))


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

