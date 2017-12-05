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

        # create the client
        self.mongo = MongoClient('mongodb://'+mongo_host)

        # Try to authenticate, will throw an exception if the user/psswd is not valid for the db
        if(mongo_user and mongo_psswd):
            self.mongo[mongo_db].authenticate(mongo_user, mongo_psswd)

        # Grab a handle to each of the databases and its collections
        self.metricsDBs = dict()
        for db in mongo_dbs:
            self.metricsDBs[db] = self.mongo[db]
            #self.userstate = self.metricsDBs[db][MongoMetricsDBI._USERSTATE]
            self.jobstate = self.db[MongoMetricsDBI._JOBSTATE]

        # Make sure we have an index on module and git_repo_url
        #self.userstate.ensure_index(('created', ASCENDING), sparse=False)
        self.jobstate.ensure_index([
            ('user', ASCENDING),
            ('created', ASCENDING),
            ('updated',ASCENDING)],
            unique=True, sparse=False)

    def list_user_jobs(self, username):
        query = {'user':username}

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
        return list(self.jobstate.find(
                        query, projection,
                        sort=[['created', ASCENDING]]))


    def get_user_job_states(self, userIds, minTime, maxTime):
        filter = {}

        userFilter = {}
        if (userIds is not None and len(userIds) > 0):
            userFilter['$in'] = userIds
        creationTimeFilter = {}
        if minTime is not None:
            creationTimeFilter['$gte'] = minTime
        if maxTime is not None:
            creationTimeFilter['$lte'] = maxTime
        if len(creationTimeFilter) > 0:
            filter['created'] = creationTimeFilter
        if len(userFilter) > 0:
            filter['user'] = userFilter

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

        return list(self.jobstate.find(filter, projection))

