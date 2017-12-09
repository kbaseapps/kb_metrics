import json
import pprint
import copy
from pymongo import MongoClient
from pymongo import ASCENDING
from pymongo import DESCENDING

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

    '''
    To get the job's 'status', 'complete'=true/false, etc., we can do joining as follows
    userjobstate.jobstate['_id']==exec_engine.exec_tasks['ujs_job_id']
    '''

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
    def list_user_tasks(self, userIds, minTime, maxTime):
        filter = {}

        userFilter = {}
        if (userIds is not None and len(userIds) > 0):
            userFilter['$in'] = userIds
        if len(userFilter) > 0:
            filter['user'] = userFilter

        creationTimeFilter = {}
        if minTime is not None:
            creationTimeFilter['$gte'] = minTime
        if maxTime is not None:
            creationTimeFilter['$lte'] = maxTime
        if len(creationTimeFilter) > 0:
            filter['creation_time'] = creationTimeFilter

        projection = {
		'user':1,
                'app_job_id':1,
                'ujs_job_id':1,
                'job_input':1,
                'job_output':1,
                'exec_start_time':1,
                'creation_time':1,
                'finish_time':1
        }

	# grab handle(s) to the database collections needed
        self.kbtasks = self.metricsDBs['exec_engine'][MongoMetricsDBI._EXEC_TASKS]

	'''
        # Make sure we have an index on user, created and updated
        self.kbtasks.ensure_index([
            ('user', ASCENDING),
            ('create', ASCENDING),
            ('login', ASCENDING)],
            unique=True, sparse=False)
	'''

        return list(self.kbtasks.find(
                        filter, projection,
                        sort=[['creation_time', ASCENDING]]))


    def list_user_details(self, userIds, minTime, maxTime):
        filter = {}

        userFilter = {}
        if (userIds is not None and len(userIds) > 0):
            userFilter['$in'] = userIds
        if len(userFilter) > 0:
            filter['user'] = userFilter

        creationTimeFilter = {}
        if minTime is not None:
            creationTimeFilter['$gte'] = minTime
        if maxTime is not None:
            creationTimeFilter['$lte'] = maxTime
        if len(creationTimeFilter) > 0:
            filter['create'] = creationTimeFilter

        projection = {
                'user':1,
                'email':1,
                'roles':1,
                'create':1,
                'login':1,
                'lastrst':1
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


    def list_user_jobs(self, userIds, minTime, maxTime):
        filter = {}

        userFilter = {}
        if (userIds is not None and len(userIds) > 0):
            userFilter['$in'] = userIds
        if len(userFilter) > 0:
            filter['user'] = userFilter

        creationTimeFilter = {}
        if minTime is not None:
            creationTimeFilter['$gte'] = minTime
        if maxTime is not None:
            creationTimeFilter['$lte'] = maxTime
        if len(creationTimeFilter) > 0:
            filter['created'] = creationTimeFilter

        projection = {
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

        return list(self.jobstate.find(
                        filter, projection#,
                        ))#sort=[['created', ASCENDING]]))


    def list_user_job_states(self, userIds, minTime, maxTime):
        filter = {}

        userFilter = {}
        if (userIds is not None and len(userIds) > 0):
            userFilter['$in'] = userIds
        if len(userFilter) > 0:
            filter['user'] = userFilter

        creationTimeFilter = {}
        if minTime is not None:
            creationTimeFilter['$gte'] = minTime
        if maxTime is not None:
            creationTimeFilter['$lte'] = maxTime
        if len(creationTimeFilter) > 0:
            filter['created'] = creationTimeFilter

        projection = {
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

