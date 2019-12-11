import datetime
from pymongo import MongoClient, DESCENDING, ASCENDING
from pymongo.errors import BulkWriteError, WriteError, ConfigurationError
from bson.objectid import ObjectId
from bson import json_util
import json
import re

from kb_Metrics.Util import _convert_to_datetime
from operator import itemgetter

def unwrap_date(obj, prop):
    if not prop in obj:
        return None
    date_value = obj[prop]
    if not isinstance(date_value, dict):
        return date_value
    if '$date' in date_value:
        return date_value['$date']
    raise ValueError('Invalid value for date "' + prop + '"')

class MongoMetricsDBI:
    '''
    MongoMetricsDBI--interface to mongodbs behind
    '''

    # Collection Names

    _DB_VERSION = 'db_version'  # single

    _USERSTATE = 'userstate'  # userjobstate.userstate
    _JOBSTATE = 'jobstate'  # userjobstate.jobstate

    _AUTH2_USERS = 'users'  # auth2.users
    _MT_USERS = 'users'  # 'test_users'#metrics.users
    _MT_DAILY_ACTIVITIES = 'daily_activities'
    _MT_NARRATIVES = 'narratives'  # metrics.narratives

    _USERPROFILES = 'profiles'  # user_profile_db.profiles

    _EXEC_APPS = 'exec_apps'  # exec_engine.exec_apps
    _EXEC_LOGS = 'exec_logs'  # exec_engine.exec_logs
    _EXEC_TASKS = 'exec_tasks'  # exec_engine.exec_tasks
    _TASK_QUEUE = 'task_queue'  # exec_engine.task_queue

    _WS_WORKSPACES = 'workspaces'  # workspace.workspaces
    _WS_WSOBJECTS = 'workspaceObjects'  # workspace.workspaceObjects

    def __init__(self, mongo_host, mongo_dbs, mongo_user, mongo_psswd):
        self.mongo_clients = dict()
        self.metricsDBs = dict()
        for m_db in mongo_dbs:
            # create the client and authenticate
            self.mongo_clients[m_db] = MongoClient(
                "mongodb://" + mongo_user + ":" + mongo_psswd +
                "@" + mongo_host + "/" + m_db)
            # grab a handle to the database
            self.metricsDBs[m_db] = self.mongo_clients[m_db][m_db]

    # Begin functions to write to the metrics database...
    def update_user_records(self, upd_filter, upd_data, kbstaff):
        """
        update_user_records--update the user info in metrics.users
        """
        upd_op = {'$currentDate': {'recordLastUpdated': True},
                  '$set': upd_data,
                  '$setOnInsert': {'kbase_staff': kbstaff}}

        # grab handle(s) to the database collection(s) targeted
        mt_users = self.metricsDBs['metrics'][MongoMetricsDBI._MT_USERS]
        update_ret = None

        # return an instance of UpdateResult(raw_result, acknowledged)
        update_ret = mt_users.update_one(upd_filter,
                                            upd_op, upsert=True)

        return update_ret

    def update_activity_records(self, upd_filter, upd_data):
        """
        update_activity_records--
        """
        upd_op = {'$currentDate': {'recordLastUpdated': True},
                  "$set": upd_data}

        # grab handle(s) to the database collection(s) targeted
        mt_coll = self.metricsDBs['metrics'][
            MongoMetricsDBI._MT_DAILY_ACTIVITIES]
        update_ret = None

        # return an instance of UpdateResult(raw_result, acknowledged)
        update_ret = mt_coll.update_one(upd_filter,
                                        upd_op, upsert=True)

        return update_ret

    def insert_activity_records(self, mt_docs):
        """
        Insert an iterable of user activity documents
        """
        if not isinstance(mt_docs, list):
            raise ValueError('Variable mt_docs must be' +
                             ' a list of mutable mapping type data.')

        # grab handle(s) to the database collection(s) targeted
        mt_act = self.metricsDBs['metrics'][
            MongoMetricsDBI._MT_DAILY_ACTIVITIES]
        insert_ret = None
        try:
            # get an instance of InsertManyResult(inserted_ids, acknowledged)
            insert_ret = mt_act.insert_many(mt_docs, ordered=False)
        except BulkWriteError as bwe:
            # skip duplicate key error (code=11000)
            panic = [x for x in bwe.details['writeErrors'] if x['code'] != 11000]
            if panic:
                print("really panic")
                raise bwe
            else:
                return bwe.details['nInserted']
        #else:
        #    # insert_ret.inserted_ids is a list
        #    print(f'Inserted {len(insert_ret.inserted_ids)} activity records.')
        return len(insert_ret.inserted_ids)

    def update_narrative_records(self, upd_filter, upd_data):
        """
        update_narrative_records--
        """
        upd_op = {'$currentDate': {'recordLastUpdated': True},
                  '$setOnInsert': {'first_access': upd_data['last_saved_at']},
                  '$set': upd_data,
                  '$inc': {'access_count': 1}}

        # grab handle(s) to the database collection(s) targeted
        mt_narrs = self.metricsDBs['metrics'][
            MongoMetricsDBI._MT_NARRATIVES]
        update_ret = None

        # return an instance of UpdateResult(raw_result, acknowledged)
        update_ret = mt_narrs.update_one(upd_filter,
                                            upd_op, upsert=True)

        # re-touch the newly inserted records
        mt_narrs.update({'access_count': {'$exists': False}},
                        {'$set': {'access_count': 1}},
                        upsert=True, multi=True)

        return update_ret
    # End functions to write to the metrics database

    # Begin functions to query the metrics dbs...
    def aggr_unique_users_per_day(self, minTime, maxTime, excluded_users=None):
        """
        aggr_unique_users_per_day: as the function name says
        """
        # excluded_users has to be an array for '$nin'
        if excluded_users is None:
            excluded_users = []

        # Define the pipeline operations
        minDate = _convert_to_datetime(minTime)
        maxDate = _convert_to_datetime(maxTime)

        match_filter = {"_id.year_mod":
                        {"$gte": minDate.year, "$lte": maxDate.year},
                        "obj_numModified": {"$gt": 0}}

        if excluded_users:
            match_filter['_id.username'] = {"$nin": excluded_users}

        pipeline = [
            {"$match": match_filter},
            {"$project": {"year_mod": "$_id.year_mod",
                          "month_mod": "$_id.month_mod",
                          "day_mod": "$_id.day_mod",
                          "username": "$_id.username", "_id": 0}},
            {"$group": {"_id": {"year_mod": "$year_mod",
                                "month_mod": "$month_mod",
                                "day_mod": "$day_mod"},
                        "numOfUsers": {"$sum": 1}}},
            {"$project": {"yyyy-mm-dd": {"$concat":
                                         [{"$substr": [
                                             "$_id.year_mod", 0, -1]}, '-',
                                          {"$substr": [
                                              "$_id.month_mod", 0, -1]}, '-',
                                          {"$substr": [
                                              "$_id.day_mod", 0, -1]}]},
                          "numOfUsers":1, "_id":0}}]

        # grab handle(s) to the db collection
        mt_acts = self.metricsDBs['metrics'][MongoMetricsDBI._MT_DAILY_ACTIVITIES]
        return sorted(list(mt_acts.aggregate(pipeline)), key=itemgetter('yyyy-mm-dd'))

    def get_user_info(self, userIds, minTime, maxTime, exclude_kbstaff=False):
        qry_filter = {}

        user_filter = {}
        if userIds:
            user_filter['$in'] = userIds
        if user_filter:
            qry_filter['username'] = user_filter

        if exclude_kbstaff:
            qry_filter['kbase_staff'] = False

        signup_time_filter = {}
        if minTime is not None:
            signup_time_filter['$gte'] = _convert_to_datetime(minTime)
        if maxTime is not None:
            signup_time_filter['$lte'] = _convert_to_datetime(maxTime)
        if signup_time_filter:
            qry_filter['signup_at'] = signup_time_filter

        projection = {
            '_id': 0,
            'username': 1,
            'email': 1,
            'full_name': 1,
            'signup_at': 1,
            'last_signin_at': 1,
            'kbase_staff': 1,
            'roles': 1
        }

        # grab handle(s) to the database collection
        mt_users = self.metricsDBs['metrics'][MongoMetricsDBI._MT_USERS]
        return sorted(list(mt_users.find(qry_filter, projection)), key=itemgetter('signup_at'))

    # End functions to query the metrics db

    # Begin functions to query the other dbs...
    def aggr_activities_from_wsobjs(self, minTime, maxTime):
        # Define the pipeline operations
        pipeline = [
            {"$match": {"moddate": {"$gte": _convert_to_datetime(minTime),
                                    "$lte": _convert_to_datetime(maxTime)}}},
            {"$project": {"year_mod": {"$year": "$moddate"},
                          "month_mod": {"$month": "$moddate"},
                          "date_mod": {"$dayOfMonth": "$moddate"},
                          "obj_name": "$name",
                          "obj_id": "$id",
                          "obj_version": "$numver",
                          "ws_id": "$ws", "_id": 0}},
            {"$group": {"_id": {"ws_id": "$ws_id",
                                "year_mod": "$year_mod",
                                "month_mod": "$month_mod",
                                "day_mod": "$date_mod"},
                        "obj_numModified": {"$sum": 1}}}]

        activities = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WSOBJECTS]
        return list(activities.aggregate(pipeline))

    def list_ws_owners(self):
        # Define the pipeline operations
        match_filter = {"cloning": {"$exists": False}}
        pipeline = [
            {"$match": match_filter},
            {"$project": {"username": "$owner",
                          "ws_id": "$ws", "name": 1, "_id": 0}}
        ]
        # grab handle(s) to the db collection
        kbworkspaces = self.metricsDBs['workspace'][
            MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    def list_narrative_info(self, wsid_list=None, wsname_list=None, owner_list=None, excluded_users=None, include_temporary=False):
        """
        list_narrative_info--retrieve the name/ws_id/owner of narratives
        of given owner/wsid filters
        """
        # has to be an array for '$in' and/or $nin'
        if wsid_list is None:
            wsid_list = []
        if owner_list is None:
            owner_list = []
        if excluded_users is None:
            excluded_users = []

        match_filter = {"del": False}

        if not include_temporary:
            match_filter['meta'] =  {"$elemMatch":
                                      {"k": "is_temporary", "v": "false"}}
        match_filter["cloning"] = {"$exists": False}

        if wsid_list:
            match_filter['ws'] = {"$in": wsid_list}
        elif wsname_list is not None:
            match_filter['name'] = {"$in": wsname_list}
        if owner_list:
            match_filter['owner'] = {"$in": owner_list}
        else:
            if excluded_users:
                match_filter["owner"] = {"$nin": excluded_users}

        # Define the pipeline operations
        pipeline = [
            {"$match": match_filter},
            {"$project": {"name": 1, "owner": 1, "ws": 1, "_id": 0,
                          "narr_keys": "$meta.k", "narr_values": "$meta.v"}}
        ]

        # grab handle(s) to the db collection
        kbworkspaces = self.metricsDBs['workspace'][
            MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    def get_workspace_info(self, wsid_list=None, wsname_list=None):
        """
        list_workspade_info--retrieve the name/ws_id/owner of a list of workspaces provided by        
        workspace id and/or name
        """
        # match_filter = {"del": True}
        match_filter = {}
        match_filter["cloning"] = {"$exists": False}

        if wsid_list:
            match_filter['ws'] = {"$in": wsid_list}
        elif wsname_list is not None:
            match_filter['name'] = {"$in": wsname_list}

        # Define the pipeline operations
        pipeline = [
            {"$match": match_filter},
            {"$project": {"name": 1, "owner": 1, "ws": 1, "_id": 0,
                          "meta_keys": "$meta.k", "meta_values": "$meta.v",
                          "del": 1}}
        ]

        # grab handle(s) to the db collection
        workspaces = self.metricsDBs['workspace'][
            MongoMetricsDBI._WS_WORKSPACES]
        return list(workspaces.aggregate(pipeline))

    def list_ws_narratives(self, minT=0, maxT=0, include_del=False):
        match_filter = {"meta": {"$elemMatch":
                                  {"k": "narrative"}}}
        match_filter["cloning"] = {"$exists": False}

        if not include_del:
            match_filter["del"] = False

        if minT > 0 and maxT > 0:
            minTime = min(minT, maxT)
            maxTime = max(minT, maxT)
            minTime = datetime.datetime.fromtimestamp(minTime / 1000.0)
            maxTime = datetime.datetime.fromtimestamp(maxTime / 1000.0)
            match_filter['moddate'] = {"$gte": minTime, "$lte": maxTime}
        elif minT > 0:
            minTime = datetime.datetime.fromtimestamp(minT / 1000.0)
            match_filter['moddate'] = {"$gte": minTime}
        elif maxT > 0:
            maxTime = datetime.datetime.fromtimestamp(maxT / 1000.0)
            match_filter['moddate'] = {"$lte": maxTime}

        # Define the pipeline operations
        pipeline = [
            {"$match": match_filter},
            {"$project": {"username": "$owner", "workspace_id": "$ws",
                          "name": 1, "narr_keys": "$meta.k",
                          "narr_values": "$meta.v",
                          "deleted": "$del", "desc": 1, "numObj": 1,
                          "last_saved_at": "$moddate", "_id": 0}}
        ]
        # grab handle(s) to the db collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    def list_more_ws_narratives(self, from_time=None, include_del=False):
        if from_time is None:
            raise(ValueError('Listing more narrativs requires a timestamp "from_time"'))

        match_filter = {"meta": {"$elemMatch":
                                  {"k": "narrative"}}}
        match_filter["cloning"] = {"$exists": False}

        if not include_del:
            match_filter["del"] = False

        minTime = datetime.datetime.fromtimestamp(from_time / 1000.0)
        match_filter['moddate'] = {"$gt": minTime}

        # Define the pipeline operations
        pipeline = [
            {"$match": match_filter},
            {"$project": {"username": "$owner", "workspace_id": "$ws",
                          "name": 1, "narr_keys": "$meta.k",
                          "narr_values": "$meta.v",
                          "deleted": "$del", "desc": 1, "numObj": 1,
                          "last_saved_at": "$moddate", "_id": 0}}
        ]
        # grab handle(s) to the db collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    def list_user_objects_from_wsobjs(self, minTime, maxTime, ws_list=None):
        """
        list_user_objects_from_wsobjs:
        """
        # has to be an array for '$in' and/or $nin'
        if ws_list is None:
            ws_list = []

        minTime = datetime.datetime.fromtimestamp(minTime / 1000.0)
        maxTime = datetime.datetime.fromtimestamp(maxTime / 1000.0)

        # Define the pipeline operations
        match_filter = {"del": False,
                        "moddate": {"$gte": minTime, "$lte": maxTime}}
        if ws_list:
            match_filter["ws"] = {"$in": ws_list}

        pipeline = [
            {"$match": match_filter},
            {"$project": {"moddate": 1, "workspace_id": "$ws",
                          "object_id": "$id", "object_name": "$name",
                          "object_version": "$numver",
                          "deleted": "$del", "_id": 0}}
        ]

        # grab handle(s) to the db collection
        kbwsobjs = self.metricsDBs['workspace'][
            MongoMetricsDBI._WS_WSOBJECTS]
        return list(kbwsobjs.aggregate(pipeline))

    def list_ws_firstAccess(self, minTime, maxTime, ws_list=None):
        """
        list_ws_firstAccess--retrieve the ws_ids and first access month (yyyy-mm)
        ("numver": 1) for workspaces/narratives as objects, the 'first_access' date is
        used for accounting narratives created at certain date.
        [{'yyyy-mm': '2016-7': 'ws_count': 1},
         {'yyyy-mm': '2017-12': 'ws_count': 1},
         {'yyyy-mm': '2018-2': 'ws_count': 7}]
        """
        # has to be an array for '$in' and/or $nin'
        if ws_list is None:
            ws_list = []

        minTime = datetime.datetime.fromtimestamp(minTime / 1000.0)
        maxTime = datetime.datetime.fromtimestamp(maxTime / 1000.0)

        # Define the pipeline operations
        match_filter = {"del": False, "numver": 1}
        if ws_list:
            match_filter["ws"] = {"$in": ws_list}

        proj1 = {"ws": 1, "moddate": 1, "_id": 0}
        grp1 = {"_id": "$ws", "first_access": {"$min": "$moddate"}}
        proj2 = {"ws": "$_id", "_id": 0, "first_access": 1}
        match_fltr2 = {"first_access": {"$gte": minTime, "$lte": maxTime}}
        proj3 = {"ws": "$_id", "_id": 0,
                 "first_access_year": {"$year": "$first_access"},
                 "first_access_month": {"$month": "$first_access"}}
        proj4 = {"ws": 1, "yyyy-mm":
                 {"$concat":
                  [{"$substr": ["$first_access_year", 0, -1]}, "-",
                   {"$substr": ["$first_access_month", 0, -1]}]}}

        grp2 = {"_id": "$yyyy-mm",
                "ws_count": {"$sum": 1}}
        proj5 = {"yyyy-mm": "$_id", "ws_count": 1, "_id": 0}

        pipeline = [
            {"$match": match_filter},
            {"$project": proj1},
            {"$group": grp1},
            {"$project": proj2},
            {"$match": match_fltr2},
            {"$project": proj3},
            {"$project": proj4},
            {"$group": grp2},
            {"$project": proj5}
        ]
        # grab handle(s) to the db collection
        kbwsobjs = self.metricsDBs['workspace'][
            MongoMetricsDBI._WS_WSOBJECTS]
        m_cursor = kbwsobjs.aggregate(pipeline)
        return list(m_cursor)

    def list_ws_lastAccess(self, minTime, maxTime, ws_list=None):
        """
        list_ws_lastAccess--retrieve the ws_ids and last access month (yyyy-mm)
        for workspaces/narratives as wsObjects, the 'last_access' date is
        used for detecting a user's most recent activities and for setting the value of
        last_signin_at in metrics.users.
        [{'yyyy-mm-dd': '2015-2-20': 'ws': 4033},
         {'yyyy-mm-dd': '2018-1-9': 'ws': 24394},
         {'yyyy-mm-dd': '2018-3-13': 'ws': 29451}]
        """
        # has to be an array for '$in' and/or $nin'
        if ws_list is None:
            ws_list = []

        minTime = datetime.datetime.fromtimestamp(minTime / 1000.0)
        maxTime = datetime.datetime.fromtimestamp(maxTime / 1000.0)

        # Define the pipeline operations
        match_filter = {"del": False,
                        "moddate": {"$gte": minTime, "$lte": maxTime}}
        if ws_list:
            match_filter["ws"] = {"$in": ws_list}

        proj1 = {"ws": 1, "moddate": 1, "_id": 0}
        grp = {"_id": "$ws", "last_access": {"$max": "$moddate"}}
        proj2 = {"ws": "$_id", "_id": 0, "last_access_date": "$last_access"}

        pipeline = [
            {"$match": match_filter},
            {"$project": proj1},
            {"$group": grp},
            {"$project": proj2}
        ]
        # grab handle(s) to the db collection
        kbwsobjs = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WSOBJECTS]
        m_cursor = kbwsobjs.aggregate(pipeline)
        return list(m_cursor)

    def list_kbstaff_usernames(self):
        kbstaff_filter = {'kbase_staff': {"$in": [True, 1]}}
        projection = {'_id': 0, 'username': 1}

        kbusers = self.metricsDBs['metrics'][MongoMetricsDBI._MT_USERS]

        return list(kbusers.find(kbstaff_filter, projection))

    def list_exec_tasks(self, jobIDs=None):
        # Must filter by job id. If job ids are left missing, it is
        # programming error
        if jobIDs is None:
            raise ValueError('Must supply JobIDs')
            
        if len(jobIDs) == 0:
            return []

        match_cond = {
            'ujs_job_id': {'$in': jobIDs}
        }

        projection = {
            '_id': 0,
            'app_job_id': 1,
            'ujs_job_id': 1,
            'creation_time': 1,
            'job_input': 1
        }

        exec_engine_db = self.metricsDBs['exec_engine'][MongoMetricsDBI._EXEC_TASKS]
        cursor = exec_engine_db.find(match_cond, projection)
        return list(cursor)

    def aggr_user_details(self, userIds, minTime, maxTime, excluded_users=None):
        # excluded_users has to be an array for '$nin'
        if excluded_users is None:
            excluded_users = []

        # Define the pipeline operations
        match_cond = {"create": {"$gte": _convert_to_datetime(minTime),
                                 "$lte": _convert_to_datetime(maxTime)}}
        if not userIds:
            match_cond["user"] = {"$nin": excluded_users}
        else:
            match_cond["user"] = {"$in": userIds, "$nin": excluded_users}

        pipeline = [
            {"$match": match_cond},
            {"$project": {"username": "$user", "email": "$email",
                          "full_name": "$display",
                          "signup_at": "$create",
                          "last_signin_at": "$login",
                          "roles": 1, "_id": 0}}]

        # grab handle(s) to the db collection
        kbusers = self.metricsDBs['auth2'][MongoMetricsDBI._AUTH2_USERS]
        return sorted(list(kbusers.aggregate(pipeline)), key=itemgetter('signup_at'))

    def aggr_signup_retn_users(self, userIds, minTime, maxTime, excluded_users=None):
        """
        aggr_signup_retn_users: count signup and returning users
        """
        # excluded_users has to be an array for '$nin'
        if excluded_users is None:
            excluded_users = []

        rtn_milis = 86400000  # 1 day
        # Define the pipeline operations
        match_cond = {"signup_at": {"$gte": _convert_to_datetime(minTime),
                                    "$lte": _convert_to_datetime(maxTime)}}
        if not userIds:
            match_cond["username"] = {"$nin": excluded_users}
        else:
            match_cond["username"] = {"$in": userIds, "$nin": excluded_users}
        match_cond["last_signin_at"] = {"$ne": None}

        pipeline = [
            {"$match": match_cond},
            {"$project": {"year_signup": {"$year": "$signup_at"},
                          "month_signup": {"$month": "$signup_at"},
                          "username": 1, "_id": 0,
                          "signin_delay": {"$subtract":
                                           ["$last_signin_at", "$signup_at"]}}},
            {"$project": {"year_signup": 1, "month_signup": 1,
                          "username": 1, "_id": 0,
                          "returning": {
                              "$cond":
                              [{"$gte": ["$signin_delay", rtn_milis]}, 1, 0]}}},
            {"$group": {"_id": {"year": "$year_signup",
                                "month": "$month_signup"},
                        "user_signups": {"$sum": 1},
                        "returning_user_count": {"$sum": "$returning"}}}]

        return list(self.metricsDBs['metrics'][MongoMetricsDBI._MT_USERS].aggregate(pipeline))

    def list_ujs_results(self, user_ids=None, start_time=None, end_time=None, job_ids=None, offset=None, limit=None, sort=None):
        filter = {}

        if user_ids:
            filter['user'] = {
                '$in': user_ids
            }

        created_filter = {}
        if start_time is not None:
            created_filter['$gte'] = _convert_to_datetime(start_time)
        if end_time is not None:
            created_filter['$lte'] = _convert_to_datetime(end_time)

        if created_filter:
            filter['created'] = created_filter

        if job_ids is not None:
            filter['_id'] = {'$in': list(map(lambda id: ObjectId(id), job_ids))}

        # qry_filter['desc'] = {'$exists': True}
        # qry_filter['status'] = {'$exists': True}

        projection = {
            'user': 1,
            'created': 1,  # datetime.datetime(2015, 1, 9, 19, 36, 8, 561000)
            'started': 1,
            'updated': 1,
            'status': 1,
            'authparam': 1,  # "DEFAULT" or workspace_id
            'authstrat': 1,  # "DEFAULT" or "kbaseworkspace"
            'complete': 1,
            'desc': 1,
            'error': 1
        }

        # grab handle(s) to the database collections needed
        jobstate = self.metricsDBs['userjobstate'][MongoMetricsDBI._JOBSTATE]
        cursor = jobstate.find(filter, projection)
        if offset is not None:
            cursor.skip(offset)
        if limit is not None:
            cursor.limit(limit)

        if sort is not None and len(sort) > 0:
            first_sort = sort[0]
            if first_sort.get('direction', None) and first_sort['direction'].startswith('desc'):
                sort_direction = DESCENDING
            else:
                sort_direction = ASCENDING
            cursor.sort(first_sort['field'], sort_direction)
        total_count = cursor.count()

        return list(cursor), total_count

    def query_ujs_total(self, users):
        find_filter = {}
        if users: 
            find_filter['user'] = {'$in': users}

        ujs_db = self.metricsDBs['userjobstate'][MongoMetricsDBI._JOBSTATE]
        cursor = ujs_db.find(find_filter)
        return cursor.count()

    def query_ujs(self, restrict_user=None, start_time=None, end_time=None, filter=None, offset=None, limit=None, sort=None, search=None):
        # Searching and filtering
        find_filter = []

        # Restrict to single user if not admin.
        if restrict_user:
            find_filter.append({'user': {'$eq': restrict_user}})

        # Time range
        # Time range is always considered and enforced. It is a search, but special.
        # TODO: this won't catch jobs started before start_time but still running;
        # I think the intention of the time range is to catch jobs from that time range,
        # which doesn't just mean started during the time range.
        # In ee2 there is an updated field which will reflect the most recent
        # lifecycle timestamp, which would take care of this. As it is, we'll need to
        # use a more complex filter.
        created_filter = {}
        if start_time is not None:
            created_filter['$gte'] = _convert_to_datetime(start_time)
        if end_time is not None:
            created_filter['$lte'] = _convert_to_datetime(end_time)

        if created_filter:
            find_filter.append({'created': created_filter})

        # Search
        # Search is match of a set of regular expressions or strings against a set of fields.
        search_filter = []
        if search is not None:
            for search_expr in search:
                comparison_type = search_expr['type']
                if comparison_type == 'regex':
                    term = re.compile(search_expr['term'])
                    term_filter = []
                    term_filter.append({'user': term})
                    try:
                        oid = ObjectId(search_expr['term'])
                        term_filter.append({'_id': {'$eq': oid}})
                    except:
                        pass
                    search_filter.append({'$or': term_filter})
                elif comparison_type == 'exact':
                    term = search_expr['term']
                    term_filter = []
                    term_filter.append({'user': {'$eq': term}})
                    try:
                        oid = ObjectId(term)
                        term_filter.append({'_id': {'$eq': oid}})
                    except:
                        pass
                    search_filter.append({'$or': term_filter})
                else:
                    raise ValueError('Invalid search term type: ' + comparison_type)
            if len(search_filter):
                find_filter.append({'$and': search_filter})

        # Filters
        # Filters are exact matches against a set of match values
        if filter is not None:
            if 'user_id' in filter:
                find_filter.append({'user': {
                    '$in': filter['user_id']
                }})

            if 'job_id' in filter:
                find_filter.append({'_id': {
                    '$in': list(map(lambda id: ObjectId(id), filter['job_id']))
                }})

            # This one is tricky - there is not usable status field, since it is used as a
            # dumping ground for various status related values.
            # That is, it does not stick to 'created', 'queued', 'running', 'error', 'cancel', 'finished'
            # or some such set of strings.

            queue_filter = {'$and': [
                {'complete': {'$eq': False}},
                {'created': {'$ne': None}},
                {'started': {'$not': {'$ne': None}}}
            ]}

            run_filter = {'$and': [
                {'complete': {'$eq': False}},
                {'started': {'$ne': None}}
            ]}

            complete_filter = {'$and': [
                {'complete': {'$eq': True}},
                {'status': {'$eq': 'done'}}
            ]}

            error_filter = {'$and': [
                {'complete': {'$eq': True}},
                {'$or': [
                    {'error': {'$eq': True}},
                    {'status': {'$eq': 'Unknown error'}},
                    {'$and': [
                        {'status': {'$ne': 'done'}},
                        {'status': {'$not': re.compile('^canceled')}}
                    ]}
                ]}
            ]}

            terminate_filter = {'$and': [
                {'complete': {'$eq': True}},
                {'status': re.compile('^canceled')}
            ]}

            if 'status' in filter:
                status_filter = []
                for status in filter['status']:
                    if status == 'queue':
                        status_filter.append(queue_filter)
                    elif status == 'run':
                        status_filter.append(run_filter)
                    elif status == 'complete':
                        status_filter.append(complete_filter)
                    elif status == 'error':
                        status_filter.append(error_filter)
                    elif status == 'terminate':
                        status_filter.append(terminate_filter)
                    # TODO: more cases!

                    if len(status_filter):
                        find_filter.append({'$or': status_filter})

        projection = {
            'user': 1,
            'created': 1,  # datetime.datetime(2015, 1, 9, 19, 36, 8, 561000)
            'started': 1,
            'updated': 1,
            'status': 1,
            'authparam': 1,  # "DEFAULT" or workspace_id
            'authstrat': 1,  # "DEFAULT" or "kbaseworkspace"
            'complete': 1,
            'desc': 1,
            'error': 1
        }

        ujs_db = self.metricsDBs['userjobstate'][MongoMetricsDBI._JOBSTATE]
        if len(find_filter):
            cursor = ujs_db.find({'$and': find_filter}, projection)
        else:
            cursor = ujs_db.find({}, projection)

        # Sorting.
        if sort is not None and len(sort) > 0:
            sorter = []
            for sort_spec in sort:
                if sort_spec.get('direction', None) and sort_spec['direction'].startswith('desc'):
                    sort_direction = DESCENDING
                else:
                    sort_direction = ASCENDING

                if sort_spec.get('field', None) in ['user', 'user_id']:
                    field = 'user'
                elif sort_spec.get('field', None) == ['job', 'job_id']:
                    field = 'job_id'
                elif sort_spec.get('field', None) == 'created':
                    field = 'created'
                elif sort_spec.get('field', None) == 'updated':
                    field = 'updated'
                else:
                    raise ValueError('Unsupported sort field: ' + sort_spec.get('field', 'n/a'))
                sorter.append( (field, sort_direction) )
            cursor.sort(sorter)

        found_count = cursor.count()

        # Offset and limit implemented simply on the cursor.
        if offset is not None:
            cursor.skip(offset)
        if limit is not None:
            cursor.limit(limit)

        # Total count.
        if restrict_user:
            total_count = self.query_ujs_total([restrict_user])
        else:
            total_count = self.query_ujs_total(None)

        return list(cursor), found_count, total_count

    def get_ujs_result(self, job_id, user_id = None):
        qry_filter = {}

        qry_filter['_id'] = ObjectId(job_id)

        if user_id is not None:
            qry_filter['user'] = user_id

        projection = {
            'user': 1,
            'created': 1,  # datetime.datetime(2015, 1, 9, 19, 36, 8, 561000)
            'started': 1,
            'updated': 1,
            'status': 1,
            'authparam': 1,  # "DEFAULT" or workspace_id
            'authstrat': 1,  # "DEFAULT" or "kbaseworkspace"
            'complete': 1,
            'desc': 1,
            'error': 1
        }

        # grab handle(s) to the database collections needed
        jobstate = self.metricsDBs['userjobstate'][MongoMetricsDBI._JOBSTATE]
        cursor = jobstate.find(qry_filter, projection)

        results = json.loads(json_util.dumps(list(cursor)))
        for r in results:
            r['_id'] = r['_id']['$oid']
            r['created'] = unwrap_date(r, 'created')
            r['updated'] = unwrap_date(r, 'updated')
            r['started'] = unwrap_date(r, 'started')

        if len(results) == 0:
            return None
        else:
            return results[0]

    # BEGIN putting the deleted functions back for reporting
    def aggr_user_logins_from_ws(self, userIds, minTime, maxTime):

        match_filter = {"moddate": {"$gte": minTime, "$lte": maxTime}}
        match_filter["cloning"] = {"$exists": False}

        if userIds:
            match_filter["owner"] = {"$in": userIds}

        # Define the pipeline operations
        pipeline = [
            {"$match": match_filter},
            {"$project": {"year": {"$year": "$moddate"},
                          "month": {"$month": "$moddate"},
                          "date": {"$dayOfMonth": "$moddate"},
                          "owner": 1, "ws_id": "$ws", "_id": 0}},
            {"$group": {"_id": {"username": "$owner",
                                "year": "$year",
                                "month": "$month"},
                        "year_mon_user_logins": {"$sum": 1}}}
        ]

        # grab handle(s) to the database collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    def aggr_total_logins(self, userIds, minTime, maxTime, excluded_users=None):
        # excluded_users has to be an array for '$nin'
        if excluded_users is None:
            excluded_users = []

        match_cond = {"moddate": {"$gte": minTime, "$lte": maxTime}}
        match_cond["cloning"] = {"$exists": False}

        if not userIds:
            match_cond["owner"] = {"$nin": excluded_users}
        else:
            match_cond["owner"] = {"$in": userIds, "$nin": excluded_users}

        # Define the pipeline operations
        pipeline = [
            {"$match": match_cond},
            {"$project": {"year": {"$year": "$moddate"},
                          "month": {"$month": "$moddate"},
                          "date": {"$dayOfMonth": "$moddate"},
                          "owner": 1, "ws_id": "$ws", "_id": 0}},
            {"$group": {"_id": {"username": "$owner",
                                "year": "$year",
                                "month": "$month"},
                        "count_user_ws_logins": {"$sum": 1}}},
            {"$group": {"_id": {"year": "$_id.year",
                                "month": "$_id.month"},
                        "year_mon_total_logins": {
                            "$sum": "$count_user_ws_logins"}}}
        ]
        # grab handle(s) to the database collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    def aggr_user_numObjs(self, userIds, minTime, maxTime):

        match_filter = {"moddate": {"$gte": minTime, "$lte": maxTime}}
        match_filter["cloning"] = {"$exists": False}

        if userIds:
            match_filter["owner"] = {"$in": userIds}

        # Define the pipeline operations
        pipeline = [
            {"$match": match_filter},
            {"$project": {"year": {"$year": "$moddate"},
                          "month": {"$month": "$moddate"},
                          "date": {"$dayOfMonth": "$moddate"},
                          "owner": 1, "ws_id": "$ws",
                          "numObj": 1, "_id": 0}},
            {"$group": {"_id": {"username": "$owner",
                                "year": "$year",
                                "month": "$month"},
                        "count_user_numObjs": {"$sum": "$numObj"}}}
        ]

        # grab handle(s) to the database collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    def aggr_user_ws(self, userIds, minTime, maxTime):
        match_filter = {"moddate": {"$gte": minTime, "$lte": maxTime}}
        match_filter["cloning"] = {"$exists": False}

        if userIds:
            match_filter["owner"] = {"$in": userIds}

        # Define the pipeline operations
        pipeline = [
            {"$match": match_filter},
            {"$project": {"year": {"$year": "$moddate"},
                          "month": {"$month": "$moddate"},
                          "date": {"$dayOfMonth": "$moddate"},
                          "owner": 1, "ws_id": "$ws", "_id": 0}},
            {"$group": {"_id": {"username": "$owner",
                                "year": "$year",
                                "month": "$month"},
                        "count_user_ws": {"$sum": 1}}}
        ]

        # grab handle(s) to the database collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    # END putting the deleted functions back for reporting

    # End functions to query the other dbs...
