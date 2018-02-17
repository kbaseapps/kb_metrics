import json
from pprint import pprint, pformat
import copy
import datetime
from pymongo import MongoClient
from pymongo import ASCENDING
from pymongo import DESCENDING
from bson.son import SON
#from pymongo import InsertOne, DeleteOne, ReplaceOne
from pymongo import ASCENDING, DESCENDING, ReturnDocument#, IndexModel
from pymongo.errors import BulkWriteError


class MongoMetricsDBI:

    # Collection Names

    _DB_VERSION='db_version' #single

    _USERSTATE='userstate'#userjobstate.userstate
    _JOBSTATE='jobstate'#userjobstate.jobstate

    _AUTH2_USERS='users'#auth2.users
    _MT_USERS='users'#'test_users'#metrics.users
    _MT_DAILY_ACTIVITIES='user_daily_activities'#'test_activities'#metrics.user_daily_activities

    _USERPROFILES='profiles'#user_profile_db.profiles

    _EXEC_APPS='exec_apps'#exec_engine.exec_apps
    _EXEC_LOGS='exec_logs'#exec_engine.exec_logs
    _EXEC_TASKS='exec_tasks'#exec_engine.exec_tasks
    _TASK_QUEUE='task_queue'#exec_engine.task_queue

    _WS_WORKSPACES='workspaces'#workspace.workspaces
    _WS_WSOBJECTS='workspaceObjects'#workspace.workspaceObjects

    def __init__(self, mongo_host, mongo_dbs, mongo_user, mongo_psswd):
        self.mongo_clients = dict()
        self.metricsDBs = dict()
        for m_db in mongo_dbs:
	    # create the client and authenticate
	    self.mongo_clients[m_db] = MongoClient("mongodb://"+mongo_user+":"+mongo_psswd
						+"@"+mongo_host+"/"+m_db)
	    # grab a handle to the database
            self.metricsDBs[m_db] = self.mongo_clients[m_db][m_db]


    ## Begin functions to write to the metrics database...
    def update_user_records(self, upd_filter, upd_data, kbstaff):
	"""
	update_user_records--
	"""
	upd_op = { 
		   '$currentDate': { 'lastUpdated': True },
		   '$set': upd_data, 
		   '$setOnInsert': {'kbase_staff': kbstaff}
		 }

	# grab handle(s) to the database collection(s) targeted
	self.mt_users = self.metricsDBs['metrics'][MongoMetricsDBI._MT_USERS]
	update_ret = None
	try:
	    #return an instance of UpdateResult(raw_result, acknowledged)
	    update_ret = self.mt_users.update_one(upd_filter, upd_op, upsert=True)
	except BulkWriteError as bwe:
	    #print("mt_users.update errored\n:")
	    #pprint(bwe.details['writeErrors'])
	    panic = filter(lambda x: x['code'] != 11000, bwe.details['writeErrors'])
	    if len(panic) > 0:
		print "really panic"
		raise
	else:
	    pass
	    #pprint(update_ret.raw_result)
	    #if update_ret.upserted_id:
		#print(update_ret.upserted_id)
	return update_ret

    def update_activity_records(self, upd_filter, upd_data):
	"""
	update_activity_records--
	"""
	upd_op = { 
		   '$currentDate': { 'lastUpdated': True },
		   "$set": upd_data
		 }

	# grab handle(s) to the database collection(s) targeted
        self.mt_coll = self.metricsDBs['metrics'][MongoMetricsDBI._MT_DAILY_ACTIVITIES]
	update_ret = None
	try:
	    #return an instance of UpdateResult(raw_result, acknowledged)
	    update_ret = self.mt_coll.update_one(upd_filter, upd_op, upsert=True)
	except BulkWriteError as bwe:
	    #pprint(bwe.details['writeErrors'])
	    panic = filter(lambda x: x['code'] != 11000, bwe.details['writeErrors'])
	    if len(panic) > 0:
		print "really panic"
		raise
	else:
	    pass
	    #pprint(update_ret.raw_result)
	    #if update_ret.upserted_id:
		#print(update_ret.upserted_id)
	return update_ret

    def insert_activity_records(self, mt_docs):
	"""
	Insert an iterable of user activity documents
	"""
	if not isinstance(mt_docs, list):
	    raise ValueError('The variable mt_docs must be a list of mutable mapping type data.')
	
	# grab handle(s) to the database collection(s) targeted
        self.mt_act = self.metricsDBs['metrics'][MongoMetricsDBI._MT_DAILY_ACTIVITIES]
	insert_ret = None
	try:
	    #get an instance of InsertManyResult(inserted_ids, acknowledged)
	    insert_ret = self.mt_act.insert_many(mt_docs, ordered=True)
	except BulkWriteError as bwe:
	    #skip uplicate key error (code=11000)
	    panic = filter(lambda x: x['code'] != 11000, bwe.details['writeErrors'])
	    if len(panic) > 0:
		print "really panic"
		raise
	else:
	    #insert_ret.inserted_ids is a list
	    pprint('Inserted {} records.'.format(len(insert_ret.inserted_ids)))
	return insert_ret



    ## End functions to write to the metrics database


    ## Begin functions to query the other dbs...
    def aggr_unique_users_per_day(self, minTime, maxTime, kbstaff=[]):
	# Define the pipeline operations
	minDate = _convert_to_datetime(minTime)
	maxDate = _convert_to_datetime(maxTime)

	match_filter = {"_id.year_mod":{"$gte":minDate.year,"$lte":maxDate.year},
		       "_id.month_mod":{"$gte":minDate.month,"$lte":maxDate.month},
		       "_id.day_mod":{"$gte":minDate.day,"$lte":maxDate.day},
		       "obj_numModified":{"$gt":0}}
	if kbstaff:
	    match_filter['_id.username'] = {"$nin":kbstaff}

	pipeline = [
	    {"$match":match_filter},
	    {"$project":{"year_mod":"$_id.year_mod", "month_mod":"$_id.month_mod","day_mod":"$_id.day_mod",
			 "username":"$_id.username","_id":0}},
	    {"$group":{"_id":{"year_mod":"$year_mod","month_mod":"$month_mod","day_mod":"$day_mod",
			"username":"$username"}}},
	    {"$group":{"_id":{"year_mod":"$_id.year_mod","month_mod":"$_id.month_mod",
			"day_mod":"$_id.day_mod"},"numOfUsers":{"$sum":1}}},
	    {"$sort":{"_id.year_mod":ASCENDING, "_id.month_mod":ASCENDING, "_id.day_mod":ASCENDING}},
	    {"$project":{"yyyy-mm-dd":{"$concat":[{"$substr":["$_id.year_mod", 0, -1 ]},'-',
			{"$substr":["$_id.month_mod", 0, -1 ]},'-',
			{"$substr":["$_id.day_mod", 0, -1 ]}]},
			"numOfUsers":1,"_id":0}}
	]

	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.mt_acts = self.metricsDBs['metrics'][MongoMetricsDBI._MT_DAILY_ACTIVITIES]
	m_cursor = self.mt_acts.aggregate(pipeline)
        return list(m_cursor)


    def get_user_info(self, userIds, minTime, maxTime, exclude_kbstaff=False):
        filter = {}

        userFilter = {}
        if (userIds is not None and len(userIds) > 0):
            userFilter['$in'] = userIds
	userFilter['$nin'] = ['kbasetest','***ROOT***','ciservices']
        if len(userFilter) > 0:
            filter['username'] = userFilter

	if exclude_kbstaff:
            filter['kbase_staff'] = False
 
        signupTimeFilter = {}
        if minTime is not None:
            signupTimeFilter['$gte'] = _convert_to_datetime(minTime)
        if maxTime is not None:
            signupTimeFilter['$lte'] = _convert_to_datetime(maxTime)
        if len(signupTimeFilter) > 0:
            filter['signup_at'] = signupTimeFilter

        projection = {
		'_id':0,
                'username':1,
                'email':1,
                'full_name':1,
                'signup_at':1,
                'last_signin_at':1,
		'kbase_staff':1,
		'roles':1
        }
	# grab handle(s) to the database collections needed
        self.mt_users = self.metricsDBs['metrics'][MongoMetricsDBI._MT_USERS]

	'''
        # Make sure we have an index on user, created and updated
        self.mt_users.ensure_index([
            ('username', ASCENDING),
            ('signup_at', ASCENDING)],
            unique=True, sparse=False)
	'''

        return list(self.mt_users.find(
                        filter, projection,
                        sort=[['signup_at', ASCENDING]]))


    ## End functions to query the metrics db


    ## Begin functions to query the other dbs...
    def aggr_activities_from_wsobjs(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year_mod":{"$year":"$moddate"},"month_mod":{"$month":"$moddate"},"date_mod":{"$dayOfMonth":"$moddate"},"obj_name":"$name","obj_id":"$id","obj_version":"$numver","ws_id":"$ws","_id":0}},
	    {"$group":{"_id":{"ws_id":"$ws_id","year_mod":"$year_mod","month_mod":"$month_mod","day_mod":"$date_mod"},"obj_numModified":{"$sum":1}}},
	    {"$sort":{"_id":ASCENDING}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WSOBJECTS]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
        return list(m_cursor)


    def aggr_activities_from_ws(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year_mod":{"$year":"$moddate"},"month_mod":{"$month":"$moddate"},"date_mod":{"$dayOfMonth":"$moddate"},"owner":1,"ws_id":"$ws","numObj":1,"_id":0}},
	    {"$group":{"_id":{"username":"$owner","year_mod":"$year_mod","month_mod":"$month_mod","day_mod":"$date_mod"},"ws_numModified":{"$sum":1},"ws_numObjs":{"$sum":"$numObj"}}},
	    {"$sort":{"_id":ASCENDING}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
        return list(m_cursor)


    def aggr_user_logins_from_ws(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws_id":"$ws","numObj":1,"meta":1,"_id":0}},
	    {"$group":{"_id":{"username":"$owner","year":"$year","month":"$month"},"year_mon_user_logins":{"$sum":1}}},
	    {"$sort":{"_id":ASCENDING}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
        return list(m_cursor)


    def aggr_total_logins(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws_id":"$ws","numObj":1,"meta":1,"_id":0}},
	    {"$group":{"_id":{"username":"$owner","year":"$year","month":"$month"},"count_user_ws_logins":{"$sum":1}}},
	    {"$group":{"_id":{"year":"$_id.year","month":"$_id.month"},"year_mon_total_logins":{"$sum":"$count_user_ws_logins"}}},
	    {"$sort":{"_id":ASCENDING}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
        return list(m_cursor)


    def aggr_user_ws(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws_id":"$ws","numObj":1,"meta":1,"_id":0}},
	    {"$group":{"_id":{"username":"$owner","year":"$year","month":"$month"},"count_user_ws":{"$sum":1}}},
	    {"$sort":{"_id":ASCENDING}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
        return list(m_cursor)


    def aggr_user_narratives(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime},"meta":{"$exists":True,"$not":{"$size":0}}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws_id":"$ws","numObj":1,"meta":1,"_id":0}},
	    {"$group":{"_id":{"username":"$owner","year":"$year","month":"$month"},"count_user_narratives":{"$sum":1}}},
	    {"$sort":{"_id":ASCENDING}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
        return list(m_cursor)


    def aggr_user_numObjs(self, minTime, maxTime):
	# Define the pipeline operations 
	pipeline = [
	    {"$match":{"moddate":{"$gte":minTime,"$lte":maxTime}}},
	    {"$project":{"year":{"$year":"$moddate"},"month":{"$month":"$moddate"},"date":{"$dayOfMonth":"$moddate"},"owner":1,"ws_id":"$ws","numObj":1,"meta":1,"_id":0}},
	    {"$group":{"_id":{"username":"$owner","year":"$year","month":"$month"},"count_user_numObjs":{"$sum":"$numObj"}}},
	    {"$sort":{"_id":ASCENDING}}
	]

	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
        return list(m_cursor)


    def list_ws_owners(self):
	# Define the pipeline operations
	pipeline = [
	    {"$match":{}},
	    {"$project":{"username":"$owner","ws_id":"$ws","name":1,"_id":0}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
        return list(m_cursor)


    def list_ws_narratives(self):
	# Define the pipeline operations
	pipeline = [
	    {"$match":{"meta":{"$exists":True,"$not":{"$size":0}}}},
	    {"$project":{"username":"$owner","ws_id":"$ws","name":1,"meta":1,"_id":0}}
	]
	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
	m_cursor = self.kbworkspaces.aggregate(pipeline)
        return list(m_cursor)


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


    def aggr_user_details(self, userIds, minTime, maxTime, excluded_users=[]):
	# Define the pipeline operations
	if userIds == []:
	    match_cond = {"$match":
				{"username":{"$nin":excluded_users},
				 "create":{"$gte":_convert_to_datetime(minTime),
					  "$lte":_convert_to_datetime(maxTime)}}
			 }
	else:
	    match_cond = {"$match":
				{"username":{"$in":userIds,"$nin":excluded_users},
				 "create":{"$gte":_convert_to_datetime(minTime),
					   "$lte":_convert_to_datetime(maxTime)}}
			 }

	pipeline = [
	    match_cond,
            {"$project":{"username":"$user","email":"$email","full_name":"$display",
			 "signup_at":"$create","last_signin_at":"$login","roles":1,"_id":0}},
	    {"$sort":{"signup_at":1}}
	]

	# grab handle(s) to the database collections needed and retrieve a MongoDB cursor
        self.kbusers = self.metricsDBs['auth2'][MongoMetricsDBI._AUTH2_USERS]
	u_cursor = self.kbusers.aggregate(pipeline)
	return list(u_cursor)


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
        self.kbusers = self.metricsDBs['auth2'][MongoMetricsDBI._AUTH2_USERS]

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

    ## End functions to query the other dbs...


## utility functions

def _datetime_from_utc(date_utc_str):
    time_format_one = '%Y-%m-%dT%H:%M:%S+0000'
    time_format_two = '%Y-%m-%dT%H:%M:%S.%fZ'
    try:#for u'2017-08-27T17:29:37+0000'
        dt = datetime.datetime.strptime(date_utc_str, time_format_one)
    except ValueError as v_er:#for ISO-formatted date & time, e.g., u'2015-02-15T22:31:47.763Z'
        dt = datetime.datetime.strptime(date_utc_str, time_format_two)
    return dt

def _convert_to_datetime(dt):
    new_dt = dt
    if (not isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime)):
        if isinstance(dt, int):# miliseconds
            new_dt = datetime.datetime.utcfromtimestamp(dt / 1000)
        else:
            new_dt = _datetime_from_utc(dt)
    return new_dt


