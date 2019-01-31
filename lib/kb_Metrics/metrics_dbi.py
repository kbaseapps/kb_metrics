import datetime
from pymongo import MongoClient
from pymongo import ASCENDING
from pymongo.errors import BulkWriteError, WriteError, ConfigurationError
from redis_cache import cache_it_json

from kb_Metrics.Util import _convert_to_datetime
from operator import itemgetter


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
            try:
                # create the client and authenticate
                self.mongo_clients[m_db] = MongoClient(
                    "mongodb://" + mongo_user + ":" + mongo_psswd +
                    "@" + mongo_host + "/" + m_db)
                # grab a handle to the database
                self.metricsDBs[m_db] = self.mongo_clients[m_db][m_db]
            except ConfigurationError as ce:
                print(ce)
                raise ce

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
        try:
            # return an instance of UpdateResult(raw_result, acknowledged)
            update_ret = mt_users.update_one(upd_filter,
                                             upd_op, upsert=True)
        except WriteError as we:
            print('WriteError caught')
            raise we
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
        try:
            # return an instance of UpdateResult(raw_result, acknowledged)
            update_ret = mt_coll.update_one(upd_filter,
                                            upd_op, upsert=True)
        except WriteError as e:
            print('WriteError caught')
            raise e
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
        else:
            # insert_ret.inserted_ids is a list
            print(f'Inserted {len(insert_ret.inserted_ids)} activity records.')
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
        try:
            # return an instance of UpdateResult(raw_result, acknowledged)
            update_ret = mt_narrs.update_one(upd_filter,
                                             upd_op, upsert=True)
        except WriteError as we:
            print('WriteError caught')
            raise we
        else:
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

        return sorted(
            list(self.metricsDBs['workspace'][MongoMetricsDBI._WS_WSOBJECTS].aggregate(pipeline)),
            key=itemgetter('_id'))

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
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

    @cache_it_json(limit=1024, expire=60 * 60 * 1)
    def list_narrative_info(self, wsid_list=None, owner_list=None, excluded_users=None):
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

        match_filter = {"del": False,
                        "meta": {"$elemMatch":
                                 {"k": "is_temporary", "v": "false"}}}
        match_filter["cloning"] = {"$exists": False}

        if wsid_list:
            match_filter['ws'] = {"$in": wsid_list}
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

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
    def list_ws_narratives(self, minT=0, maxT=0, include_del=False):
        match_filter = {"meta": {"$elemMatch":
                                 {"$or":
                                  [{"k": "narrative"},
                                   {"k": "narrative_nice_name"}]}}}
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

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
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

    @cache_it_json(limit=1024, expire=60 * 60 * 24)
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

    @cache_it_json(limit=1024, expire=60 * 60 * 24)
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

    @cache_it_json(limit=1024, expire=60 * 60 * 7 * 24)
    def list_kbstaff_usernames(self):
        kbstaff_filter = {'kbase_staff': {"$in": [True, 1]}}
        projection = {'_id': 0, 'username': 1}

        kbusers = self.metricsDBs['metrics'][MongoMetricsDBI._MT_USERS]

        return list(kbusers.find(kbstaff_filter, projection))

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
    def list_exec_tasks(self, minTime, maxTime):
        qry_filter = {}

        creation_time_filter = {}
        if minTime:
            creation_time_filter['$gte'] = minTime
        if maxTime:
            creation_time_filter['$lte'] = maxTime
        if creation_time_filter:
            qry_filter['creation_time'] = creation_time_filter

        projection = {
            '_id': 0,
            'app_job_id': 1,
            'ujs_job_id': 1,
            'creation_time': 1,
            'job_input': 1
        }
        # grab handle(s) to the database collection
        kbtasks = self.metricsDBs['exec_engine'][MongoMetricsDBI._EXEC_TASKS]
        return sorted(list(kbtasks.find(qry_filter, projection)),
                      key=itemgetter('creation_time'))

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
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

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
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

        # grab handle(s) to the db collection
        mtusers = self.metricsDBs['metrics'][MongoMetricsDBI._MT_USERS]
        return sorted(list(mtusers.aggregate(pipeline)), key=itemgetter('_id'))

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
    def list_ujs_results(self, userIds, minTime, maxTime):
        qry_filter = {}

        user_filter = {}
        if userIds:
            user_filter['$in'] = userIds
        if user_filter:
            qry_filter['user'] = user_filter

        created_filter = {}
        if minTime:
            created_filter['$gte'] = _convert_to_datetime(minTime)
        if maxTime:
            created_filter['$lte'] = _convert_to_datetime(maxTime)
        if created_filter:
            qry_filter['created'] = created_filter
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
        return list(jobstate.find(qry_filter, projection))

    # BEGIN putting the deleted functions back for reporting
    @cache_it_json(limit=1024, expire=60 * 60 / 2)
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
                        "year_mon_user_logins": {"$sum": 1}}},
            {"$sort": {"_id": ASCENDING}}
        ]

        # grab handle(s) to the database collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
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
                            "$sum": "$count_user_ws_logins"}}},
            {"$sort": {"_id": ASCENDING}}
        ]
        # grab handle(s) to the database collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
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
                        "count_user_numObjs": {"$sum": "$numObj"}}},
            {"$sort": {"_id": ASCENDING}}
        ]

        # grab handle(s) to the database collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    @cache_it_json(limit=1024, expire=60 * 60 / 2)
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
                        "count_user_ws": {"$sum": 1}}},
            {"$sort": {"_id": ASCENDING}}
        ]

        # grab handle(s) to the database collection
        kbworkspaces = self.metricsDBs['workspace'][MongoMetricsDBI._WS_WORKSPACES]
        return list(kbworkspaces.aggregate(pipeline))

    # END putting the deleted functions back for reporting

    # End functions to query the other dbs...
