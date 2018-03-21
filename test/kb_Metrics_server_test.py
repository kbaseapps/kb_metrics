# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
import datetime
import copy
from pymongo import MongoClient

from os import environ
try:
    from ConfigParser import ConfigParser  # py2
except ImportError:
    from configparser import ConfigParser  # py3

from pprint import pprint, pformat  # noqa: F401

from biokbase.workspace.client import Workspace as workspaceService
from kb_Metrics.kb_MetricsImpl import kb_Metrics
from kb_Metrics.kb_MetricsServer import MethodContext
from kb_Metrics.authclient import KBaseAuth as _KBaseAuth
from kb_Metrics.metricsdb_controller import MetricsMongoDBController
from kb_Metrics.metricsDBs import MongoMetricsDBI


class kb_MetricsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        token = environ.get('KB_AUTH_TOKEN', None)
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('kb_Metrics'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)
        cls.ctx.update({'token': token,
                        'user_id': user_id,
                        'provenance': [
                            {'service': 'kb_Metrics',
                             'method': 'please_never_use_it_in_production',
                             'method_params': []
                             }],
                        'authenticated': 1})
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = workspaceService(cls.wsURL)
        cls.serviceImpl = kb_Metrics(cls.cfg)
        cls.scratch = cls.cfg['scratch']
        cls.callback_url = os.environ['SDK_CALLBACK_URL']
        cls.db_controller = MetricsMongoDBController(cls.cfg)
        cls.client = MongoClient(port=27017)
        cls.init_mongodb()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    @classmethod
    def init_mongodb(cls):
        print('starting to build local mongoDB')

        os.system("sudo service mongodb start")
        os.system("mongod --version")
        os.system("cat /var/log/mongodb/mongodb.log "
                  "| grep 'waiting for connections on port 27017'")

        cls._insert_data(cls.client, 'workspace', 'workspaces')
        cls._insert_data(cls.client, 'exec_engine', 'exec_tasks')
        cls._insert_data(cls.client, 'userjobstate', 'jobstate')
        cls._insert_data(cls.client, 'workspace', 'workspaceObjects')
        cls._insert_data(cls.client, 'auth2', 'users')
        cls._insert_data(cls.client, 'metrics', 'users')
        cls._insert_data(cls.client, 'metrics', 'daily_activities')
        cls._insert_data(cls.client, 'metrics', 'narratives')
        cls.db_names = cls.client.database_names()

        # updating created to timstamp field for userjobstate.jobstate
        for jrecord in cls.client.userjobstate.jobstate.find():
            created_str = jrecord.get('created')
            cls.client.userjobstate.jobstate.update_many(
                {"created": created_str},
                {"$set": {"created": datetime.datetime.utcfromtimestamp(
                                        int(created_str) / 1000.0)}}
            )
        # updating data fields from timstamp to datetime.datetime format
        db_coll1 = cls.client.workspace.workspaceObjects
        for wrecord in db_coll1.find():
            moddate_str = wrecord.get('moddate')
            if type(moddate_str) not in [datetime.date, datetime.datetime]:
                moddate = datetime.datetime.utcfromtimestamp(
                                        int(moddate_str) / 1000.0)
                db_coll1.update_many(
                    {"moddate": moddate_str},
                    {"$set": {"moddate": moddate}},
                    upsert=False
                )

        db_coll2 = cls.client.workspace.workspaces
        for wrecord in db_coll2.find():
            moddate_str = wrecord.get('moddate')
            if type(moddate_str) not in [datetime.date, datetime.datetime]:
                moddate = datetime.datetime.utcfromtimestamp(
                                        int(moddate_str) / 1000.0)
                db_coll2.update_many(
                    {"moddate": moddate_str},
                    {"$set": {"moddate": moddate}},
                    upsert=False
                )

        db_coll3 = cls.client.metrics.users
        for urecord in db_coll3.find():
            signup_at_str = urecord.get('signup_at')
            last_signin_at_str = urecord.get('last_signin_at')
            if type(signup_at_str) not in [datetime.date, datetime.datetime]:
                signup_date = datetime.datetime.utcfromtimestamp(
                                    int(signup_at_str) / 1000.0)
                signin_date = datetime.datetime.utcfromtimestamp(
                                    int(last_signin_at_str) / 1000.0)
                db_coll3.update_many(
                    {"signup_at": signup_at_str,
                     "last_signin_at": last_signin_at_str},
                    {"$set": {"signup_at": signup_date,
                              "last_signin_at": signin_date}},
                    upsert=False
                )

        db_coll4 = cls.client.metrics.narratives
        for urecord in db_coll4.find():
            first_acc_str = urecord.get('first_access')
            last_saved_at_str = urecord.get('last_saved_at')
            if type(first_acc_str) not in [datetime.date, datetime.datetime]:
                first_acc_date = datetime.datetime.utcfromtimestamp(
                                    int(first_acc_str) / 1000.0)
                last_saved_date = datetime.datetime.utcfromtimestamp(
                                    int(last_saved_at_str) / 1000.0)
                db_coll4.update_many(
                    {"first_access": first_acc_str,
                     "last_saved_at": last_saved_at_str},
                    {"$set": {"first_access": first_acc_date,
                              "last_saved_at": last_saved_date}},
                    upsert=False
                )

        db_coll_au = cls.client.auth2.users
        for urecord in db_coll_au.find():
            create_str = urecord.get('create')
            login_str = urecord.get('login')
            if type(create_str) not in [datetime.date, datetime.datetime]:
                db_coll_au.update_many(
                    {"create": create_str, "login": login_str},
                    {"$set": {"create": datetime.datetime.utcfromtimestamp(
                                            int(create_str) / 1000.0),
                              "login": datetime.datetime.utcfromtimestamp(
                                            int(login_str) / 1000.0)}},
                    upsert=False
                )

        db_names = cls.client.database_names()
        for db in db_names:
            if db != 'local':
                cls.client[db].command("createUser", "admin",
                                       pwd="password", roles=["readWrite"])

        cls.dbi = MongoMetricsDBI('', db_names, 'admin', 'password')

    @classmethod
    def _insert_data(cls, client, db_name, table):

        db = client[db_name]

        record_file = os.path.join('db_files',
                                   'ci_{}.{}.json'.format(db_name, table))
        json_data = open(record_file).read()
        records = json.loads(json_data)

        db[table].drop()
        db[table].insert_many(records)
        print('Inserted {} records for {}.{}'.format(len(records),
                                                     db_name, table))

    def getWsClient(self):
        return self.__class__.wsClient

    def getWsName(self):
        if hasattr(self.__class__, 'wsName'):
            return self.__class__.wsName
        suffix = int(time.time() * 1000)
        wsName = "test_kb_Metrics_" + str(suffix)
        ret = self.getWsClient().create_workspace({'workspace': wsName})  # noqa
        self.__class__.wsName = wsName
        return wsName

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_constructor")
    def test_MetricsMongoDBs_constructor(self):
        # testing if the db is connected and handshakes cab be made
        exec_cur = self.dbi.metricsDBs['exec_engine']['exec_tasks'].find()
        self.assertEqual(len(list(exec_cur)), 84)
        ws_cur = self.dbi.metricsDBs['workspace']['workspaces'].find()
        self.assertEqual(len(list(ws_cur)), 27)
        wsobj_cur = self.dbi.metricsDBs['workspace']['workspaceObjects'].find()
        self.assertEqual(len(list(wsobj_cur)), 41)
        ujs_cur = self.dbi.metricsDBs['userjobstate']['jobstate'].find()
        self.assertEqual(len(list(ujs_cur)), 36)
        a_users_cur = self.dbi.metricsDBs['auth2']['users'].find()
        self.assertEqual(len(list(a_users_cur)), 37)
        m_users_cur = self.dbi.metricsDBs['metrics']['users'].find()
        self.assertTrue(len(list(m_users_cur)), 37)
        act_cur = self.dbi.metricsDBs['metrics']['daily_activities'].find()
        self.assertTrue(len(list(act_cur)) >= 1603)
        narrs_cur = self.dbi.metricsDBs['metrics']['narratives'].find()
        self.assertTrue(len(list(narrs_cur)), 10)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_exec_tasks")
    def test_MetricsMongoDBs_list_exec_tasks(self):
        minTime = 1500000932952
        maxTime = 1500046845591

        # testing list_exec_tasks return data
        exec_tasks = self.dbi.list_exec_tasks(minTime, maxTime)
        self.assertEqual(len(exec_tasks), 3)
        for tsk in exec_tasks:
            self.assertTrue(minTime <= tsk['creation_time'] <= maxTime)
        self.assertEqual(exec_tasks[0]['ujs_job_id'],
                         '596832a4e4b08b65f9ff5d6f')
        self.assertEqual(exec_tasks[0]['job_input']['wsid'], 15206)
        self.assertEqual(exec_tasks[1]['ujs_job_id'],
                         '5968cd75e4b08b65f9ff5d7c')
        self.assertNotIn('wsid', exec_tasks[1]['job_input'])
        self.assertEqual(exec_tasks[2]['ujs_job_id'],
                         '5968e5fde4b08b65f9ff5d7d')
        self.assertEqual(exec_tasks[2]['job_input']['wsid'], 23165)

    # Uncomment to skip this test
    # @unittest.skip("skipped MetricsMongoDBs_list_user_objects_from_wsobjs")
    def test_MetricsMongoDBs_list_user_objects_from_wsobjs(self):
        minTime = 1468592344887
        maxTime = 1519768865840
        ws_narrs = self.dbi.list_ws_narratives()
        ws_list = [wn['workspace_id'] for wn in ws_narrs]

        # test list_user_objects_from_wsobjs return count without wsid
        user_objs = self.dbi.list_user_objects_from_wsobjs(
                        minTime, maxTime)
        self.assertEqual(len(user_objs), 37)

        # test list_user_objects_from_wsobjs returned values with wsid filter
        user_objs = self.dbi.list_user_objects_from_wsobjs(
                        minTime, maxTime, ws_list)
        self.assertEqual(len(user_objs), 22)

        self.assertIn('workspace_id', user_objs[0])
        self.assertIn('object_id', user_objs[0])
        self.assertIn('object_name', user_objs[0])
        self.assertIn('object_version', user_objs[0])
        self.assertIn('moddate', user_objs[0])
        self.assertIn('deleted', user_objs[0])

        self.assertEqual(user_objs[1]['workspace_id'], 8768)
        self.assertEqual(user_objs[1]['object_id'], 12)
        self.assertEqual(user_objs[1]['object_name'],
                         'Align_Reads_using_Bowtie2_0x242ac110001L')
        self.assertEqual(user_objs[1]['object_version'], 1)
        self.assertEqual(user_objs[1]['moddate'],
                         datetime.datetime(2016, 7, 15, 14, 19, 4, 915000))
        self.assertFalse(user_objs[1]['deleted'])

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_ws_owners")
    def test_MetricsMongoDBs_list_ws_owners(self):
        # testing list_user_objects_from_wsobjs return data
        ws_owners = self.dbi.list_ws_owners()
        self.assertEqual(len(ws_owners), 27)
        self.assertIn('ws_id', ws_owners[0])
        self.assertIn('username', ws_owners[0])
        self.assertIn('name', ws_owners[0])

        self.assertEqual(ws_owners[1]['ws_id'], 7645)
        self.assertIn(ws_owners[1]['username'], 'jplfaria')
        self.assertIn(ws_owners[1]['name'], 'jplfaria:1464632279763')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_aggr_user_details")
    def test_MetricsMongoDBs_aggr_user_details(self):
        minTime = 1516307704700
        maxTime = 1520549345000
        user_list0 = []
        user_list = ['shahmaneshb', 'laramyenders', 'allmon', 'boris']

        # testing aggr_user_details_returned data structure
        users = self.dbi.aggr_user_details(user_list, minTime, maxTime)
        self.assertEqual(len(users), 4)
        self.assertIn('username', users[0])
        self.assertIn('email', users[0])
        self.assertIn('full_name', users[0])
        self.assertIn('signup_at', users[0])
        self.assertIn('last_signin_at', users[0])
        self.assertIn('roles', users[0])
        # testing the expected values
        self.assertEqual(users[1]['username'], 'laramyenders')
        self.assertEqual(users[1]['email'], 'laramy.enders@gmail.com')
        self.assertEqual(users[1]['full_name'], 'Laramy Enders')
        self.assertEqual(users[1]['signup_at'],
                         datetime.datetime(2018, 2, 16, 15, 19, 55, 973000))
        self.assertEqual(users[1]['last_signin_at'],
                         datetime.datetime(2018, 2, 16, 15, 19, 56, 426000))
        self.assertEqual(users[1]['roles'], [])

        users = self.dbi.aggr_user_details(user_list0, minTime, maxTime)
        self.assertEqual(len(users), 37)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_aggr_unique_users_per_day")
    def test_MetricsMongoDBs_aggr_unique_users_per_day(self):
        minTime = 1514764800000
        maxTime = 1522454400000

        # testing aggr_unique_users_per_day return data
        users = self.dbi.aggr_unique_users_per_day(minTime, maxTime)
        self.assertEqual(len(users), 59)
        self.assertIn('numOfUsers', users[0])
        self.assertIn('yyyy-mm-dd', users[0])
        self.assertEqual(users[0]['yyyy-mm-dd'], '2018-1-1')
        self.assertEqual(users[0]['numOfUsers'], 1)
        self.assertEqual(users[1]['numOfUsers'], 4)
        self.assertEqual(users[2]['numOfUsers'], 6)
        self.assertEqual(users[3]['numOfUsers'], 8)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_update_user_records")
    def test_MetricsMongoDBs_update_user_records(self):
        dt1 = datetime.datetime(2018, 3, 12, 1, 13, 30)
        dt2 = datetime.datetime(2018, 3, 12, 1, 35, 30)
        upd_filter_set = {'username': 'test_u1', 'email': 'test_e1'}
        upd_data_set = {'full_name': 'test_nm1', 'roles': [],
                        'signup_at': dt1, 'last_signin_at': dt2}
        isKBstaff = False

        # record does not exist (yet)
        db_mu = self.client.metrics.users
        assert db_mu.find_one(upd_filter_set) is None

        # testing freshly upserted result
        upd_ret = self.dbi.update_user_records(upd_filter_set,
                                               upd_data_set, isKBstaff)

        self.assertTrue(upd_ret.raw_result.get('upserted'))
        self.assertFalse(upd_ret.raw_result.get('updatedExisting'))
        self.assertTrue(upd_ret.raw_result.get('ok'))
        self.assertEqual(upd_ret.raw_result.get('n'), 1)

        murecord = db_mu.find_one({'username': 'test_u1',
                                   'email': 'test_e1'})
        self.assertEqual(murecord['full_name'], 'test_nm1')
        self.assertEqual(murecord['roles'], [])
        self.assertEqual(murecord['signup_at'], dt1)
        self.assertEqual(murecord['last_signin_at'], dt2)
        self.assertEqual(murecord['kbase_staff'], isKBstaff)

        # testing updating existing record
        dt2 = datetime.datetime(2018, 3, 13, 1, 35, 30)
        upd_data_set = {'full_name': 'test_nm1', 'roles': [],
                        'signup_at': dt1, 'last_signin_at': dt2}

        upd_ret = self.dbi.update_user_records(upd_filter_set,
                                               upd_data_set, isKBstaff)

        self.assertFalse(upd_ret.raw_result.get('upserted'))
        self.assertTrue(upd_ret.raw_result.get('updatedExisting'))
        self.assertTrue(upd_ret.raw_result.get('ok'))
        self.assertEqual(upd_ret.raw_result.get('n'), 1)

        murecord = db_mu.find_one({'username': 'test_u1',
                                   'email': 'test_e1'})
        self.assertEqual(murecord['full_name'], 'test_nm1')
        self.assertEqual(murecord['roles'], [])
        self.assertEqual(murecord['signup_at'], dt1)
        self.assertEqual(murecord['last_signin_at'], dt2)
        self.assertEqual(murecord['kbase_staff'], isKBstaff)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_insert_activity_records")
    def test_MetricsMongoDBs_insert_activity_records(self):
        # Fake data
        act_set = [{'_id': {'username': 'qz',
                            'year_mod': 2019,
                            'month_mod': 1,
                            'day_mod': 1,
                            'ws_id': 20199994},
                    'obj_numModified': 94},
                   {'_id': {'username': 'qz',
                            'year_mod': 2019,
                            'month_mod': 1,
                            'day_mod': 2,
                            'ws_id': 20199995},
                    'obj_numModified': 95}]

        filter_set1 = {'_id.username': 'qz',
                       '_id.year_mod': 2019,
                       '_id.month_mod': 1,
                       '_id.day_mod': 1,
                       '_id.ws_id': 20199994}
        filter_set2 = {'_id.username': 'qz',
                       '_id.year_mod': 2019,
                       '_id.month_mod': 1,
                       '_id.day_mod': 2,
                       '_id.ws_id': 20199995}

        db_mda = self.client.metrics.daily_activities
        # before insert
        assert db_mda.find_one(filter_set1) is None
        assert db_mda.find_one(filter_set2) is None

        # testing freshly inserted result
        inst_ret = self.dbi.insert_activity_records(act_set)
        self.assertEqual(inst_ret, 2)
        self.assertEqual(len(list(db_mda.find(filter_set1))), 1)
        self.assertEqual(len(list(db_mda.find(filter_set2))), 1)

        mdarecord = db_mda.find_one(filter_set1)
        self.assertEqual(mdarecord['_id']['ws_id'], 20199994)
        self.assertEqual(mdarecord['obj_numModified'], 94)

        mdarecord = db_mda.find_one(filter_set2)
        self.assertEqual(mdarecord['_id']['year_mod'], 2019)
        self.assertEqual(mdarecord['_id']['ws_id'], 20199995)
        self.assertEqual(mdarecord['obj_numModified'], 95)

        # test inserting data set with duplicates already in db
        act_set1 = [{'_id': {'username': 'qz',
                             'year_mod': 2019,
                             'month_mod': 1,
                             'day_mod': 1,
                             'ws_id': 20199994},
                     'obj_numModified': 94},
                    {'_id': {'username': 'qz1',
                             'year_mod': 2019,
                             'month_mod': 3,
                             'day_mod': 1,
                             'ws_id': 20199998},
                     'obj_numModified': 99},
                    {'_id': {'username': 'qz',
                             'year_mod': 2019,
                             'month_mod': 1,
                             'day_mod': 2,
                             'ws_id': 20199995},
                     'obj_numModified': 95},
                    {'_id': {'username': 'qz1',
                             'year_mod': 2019,
                             'month_mod': 3,
                             'day_mod': 2,
                             'ws_id': 20199999},
                     'obj_numModified': 100}]

        filter_set3 = {'_id.username': 'qz1',
                       '_id.year_mod': 2019,
                       '_id.month_mod': 3,
                       '_id.day_mod': 1,
                       '_id.ws_id': 20199998}
        filter_set4 = {'_id.username': 'qz1',
                       '_id.year_mod': 2019,
                       '_id.month_mod': 3,
                       '_id.day_mod': 2,
                       '_id.ws_id': 20199999}

        # before insert
        assert db_mda.find_one(filter_set3) is None
        assert db_mda.find_one(filter_set4) is None

        # testing freshly inserted result, skipped exisiting entries
        inst_ret1 = self.dbi.insert_activity_records(act_set1)
        self.assertEqual(inst_ret1, 2)
        self.assertEqual(len(list(db_mda.find(filter_set1))), 1)
        self.assertEqual(len(list(db_mda.find(filter_set2))), 1)
        self.assertEqual(len(list(db_mda.find(filter_set3))), 1)
        self.assertEqual(len(list(db_mda.find(filter_set4))), 1)

        # existing records intact
        mdarecord = db_mda.find_one(filter_set1)
        self.assertEqual(mdarecord['_id']['ws_id'], 20199994)
        self.assertEqual(mdarecord['obj_numModified'], 94)

        mdarecord = db_mda.find_one(filter_set2)
        self.assertEqual(mdarecord['_id']['year_mod'], 2019)
        self.assertEqual(mdarecord['_id']['ws_id'], 20199995)
        self.assertEqual(mdarecord['obj_numModified'], 95)

        # two new records inserted
        for mdarecord in db_mda.find(filter_set3):
            self.assertEqual(mdarecord['_id']['username'], 'qz1')
            self.assertEqual(mdarecord['_id']['month_mod'], 3)
            self.assertEqual(mdarecord['_id']['ws_id'], 20199998)
            self.assertEqual(mdarecord['obj_numModified'], 99)
        for mdarecord in db_mda.find(filter_set4):
            self.assertEqual(mdarecord['_id']['username'], 'qz1')
            self.assertEqual(mdarecord['_id']['month_mod'], 3)
            self.assertEqual(mdarecord['_id']['ws_id'], 20199999)
            self.assertEqual(mdarecord['obj_numModified'], 100)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_update_activity_records")
    def test_MetricsMongoDBs_update_activity_records(self):
        # Fake data
        upd_filter_set1 = {'_id.username': 'qz',
                           '_id.year_mod': 2019,
                           '_id.month_mod': 1,
                           '_id.day_mod': 1,
                           '_id.ws_id': 20199991}
        upd_data_set1 = {'obj_numModified': 91}

        upd_filter_set2 = {'_id.username': 'qz',
                           '_id.year_mod': 2019,
                           '_id.month_mod': 1,
                           '_id.day_mod': 2,
                           '_id.ws_id': 20199992}
        upd_data_set2 = {'obj_numModified': 92}
        upd_data_set3 = {'obj_numModified': 93}

        db_mda = self.client.metrics.daily_activities
        assert db_mda.find_one(upd_filter_set1) is None
        assert db_mda.find_one(upd_filter_set2) is None

        # testing freshly upserted result
        upd_ret = self.dbi.update_activity_records(upd_filter_set1,
                                                   upd_data_set1)
        self.assertTrue(upd_ret.raw_result.get('upserted'))
        self.assertFalse(upd_ret.raw_result.get('updatedExisting'))
        self.assertTrue(upd_ret.raw_result.get('ok'))
        self.assertEqual(upd_ret.raw_result.get('n'), 1)
        self.assertEqual(len(list(db_mda.find(upd_filter_set1))), 1)

        upd_ret = self.dbi.update_activity_records(upd_filter_set2,
                                                   upd_data_set2)
        self.assertEqual(len(list(db_mda.find(upd_filter_set2))), 1)

        self.assertEqual(db_mda.find_one(
                         upd_filter_set1)['obj_numModified'], 91)
        self.assertEqual(db_mda.find_one(
                         upd_filter_set2)['obj_numModified'], 92)

        # testing updating existing record
        upd_ret = self.dbi.update_activity_records(upd_filter_set2,
                                                   upd_data_set3)
        self.assertFalse(upd_ret.raw_result.get('upserted'))
        self.assertTrue(upd_ret.raw_result.get('updatedExisting'))
        self.assertTrue(upd_ret.raw_result.get('ok'))
        self.assertEqual(upd_ret.raw_result.get('n'), 1)
        self.assertEqual(db_mda.find_one(
                         upd_filter_set2)['obj_numModified'], 93)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_update_narrative_records")
    def test_MetricsMongoDBs_update_narrative_records(self):
        # Fake data
        upd_filter_set1 = {'object_id': 1,
                           'object_version': 1,
                           'workspace_id': 20199991}
        upd_data_set1 = {'name': 'qz1:narrative_1548358542000',
                         'last_saved_at': datetime.datetime(
                                                2019, 1, 24, 19, 35, 42),
                         'last_saved_by': 'qz1',
                         'numObj': 3,
                         'nice_name': 'nice_to_have',
                         'desc': '',
                         'deleted': False}

        upd_filter_set2 = {'object_id': 1,
                           'object_version': 1,
                           'workspace_id': 20199992}
        upd_data_set2 = {'name': 'qz1:narrative_1548358542100',
                         'last_saved_at': datetime.datetime(
                                                2019, 1, 24, 19, 35, 42, 1000),
                         'last_saved_by': 'qz1',
                         'numObj': 5,
                         'nice_name': 'nice_to_have_2',
                         'desc': 'temp_desc',
                         'deleted': False}

        upd_data_set3 = {'last_saved_at': datetime.datetime(
                                                2019, 1, 24, 19, 35, 48, 1000),
                         'numObj': 7}

        db_mn = self.client.metrics.narratives
        assert db_mn.find_one(upd_filter_set1) is None
        assert db_mn.find_one(upd_filter_set2) is None

        # testing freshly upserted result
        upd_ret = self.dbi.update_narrative_records(upd_filter_set1,
                                                    upd_data_set1)
        self.assertTrue(upd_ret.raw_result.get('upserted'))
        self.assertFalse(upd_ret.raw_result.get('updatedExisting'))
        self.assertTrue(upd_ret.raw_result.get('ok'))
        self.assertEqual(upd_ret.raw_result.get('n'), 1)
        self.assertEqual(len(list(db_mn.find(upd_filter_set1))), 1)

        upd_ret = self.dbi.update_narrative_records(upd_filter_set2,
                                                    upd_data_set2)
        self.assertEqual(len(list(db_mn.find(upd_filter_set2))), 1)

        mnrecord = db_mn.find_one(upd_filter_set1)
        self.assertEqual(mnrecord['nice_name'], 'nice_to_have')
        self.assertEqual(mnrecord['numObj'], 3)

        mnrecord = db_mn.find_one(upd_filter_set2)
        self.assertEqual(mnrecord['nice_name'], 'nice_to_have_2')
        self.assertEqual(mnrecord['numObj'], 5)

        # testing updating existing record
        upd_ret = self.dbi.update_narrative_records(upd_filter_set2,
                                                    upd_data_set3)
        self.assertFalse(upd_ret.raw_result.get('upserted'))
        self.assertTrue(upd_ret.raw_result.get('updatedExisting'))
        self.assertTrue(upd_ret.raw_result.get('ok'))
        self.assertEqual(upd_ret.raw_result.get('n'), 1)

        mnrecord = db_mn.find_one(upd_filter_set2)
        self.assertEqual(mnrecord['last_saved_at'],
                         datetime.datetime(2019, 1, 24, 19, 35, 48, 1000))
        self.assertEqual(mnrecord['numObj'], 7)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_get_user_info")
    def test_MetricsMongoDBs_get_user_info(self):
        minTime = 1516307704700
        maxTime = 1520549345000
        user_list0 = []
        user_list = ['shahmaneshb', 'laramyenders', 'allmon', 'boris']

        # testing get_user_info return data
        users = self.dbi.get_user_info(user_list0, minTime, maxTime)
        self.assertEqual(len(users), 37)
        users = self.dbi.get_user_info(user_list, minTime, maxTime)
        self.assertEqual(len(users), 4)
        self.assertIn('username', users[0])
        self.assertIn('email', users[0])
        self.assertIn('full_name', users[0])
        self.assertIn('signup_at', users[0])
        self.assertIn('last_signin_at', users[0])
        self.assertIn('roles', users[0])

    # Uncomment to skip this test
    # @unittest.skip("skipped MetricsMongoDBs_aggr_activities_from_wsobjs")
    def test_MetricsMongoDBs_aggr_activities_from_wsobjs(self):
        # testing aggr_activities_from_wsobjs return data
        minTime = 1468540813000
        maxTime = 1519768865840

        user_acts = self.dbi.aggr_activities_from_wsobjs(minTime, maxTime)
        self.assertEqual(len(user_acts), 11)
        self.assertEqual(user_acts[4]['_id']['ws_id'], 29624)
        self.assertEqual(user_acts[4]['_id']['year_mod'], 2018)
        self.assertEqual(user_acts[4]['_id']['month_mod'], 2)
        self.assertEqual(user_acts[4]['_id']['day_mod'], 26)
        self.assertEqual(user_acts[4]['obj_numModified'], 5)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_ws_narratives")
    def test_MetricsMongoDBs_list_ws_narratives(self):
        minTime = 1468592344887
        maxTime = 1519768865840

        # Testing with time limit
        ws_narrs = self.dbi.list_ws_narratives(minTime, maxTime)
        self.assertEqual(len(ws_narrs), 10)

        # Testing without given time limit
        ws_narrs = self.dbi.list_ws_narratives()

        # Ensure 'narrative_nice_name' exists in 'meta'
        for wn in ws_narrs:
            self.assertIn('meta', wn)
            self.assertTrue(any(d['k'] == 'narrative_nice_name'
                                for d in wn['meta']))

        self.assertEqual(len(ws_narrs), 24)
        self.assertIn('username', ws_narrs[0])
        self.assertIn('workspace_id', ws_narrs[0])
        self.assertIn('name', ws_narrs[0])
        self.assertIn('deleted', ws_narrs[0])
        self.assertIn('desc', ws_narrs[0])
        self.assertIn('numObj', ws_narrs[0])
        self.assertIn('last_saved_at', ws_narrs[0])

        self.assertEqual(ws_narrs[23]['username'], 'psdehal')
        self.assertEqual(ws_narrs[23]['workspace_id'], 27834)
        self.assertEqual(ws_narrs[23]['name'],
                         'psdehal:narrative_1513709108341')
        self.assertEqual(ws_narrs[23]['meta'][2]['k'], 'narrative_nice_name')
        self.assertEqual(ws_narrs[23]['meta'][2]['v'], 'Staging Test')
        self.assertFalse(ws_narrs[23]['deleted'])
        self.assertEqual(ws_narrs[23]['desc'], '')
        self.assertEqual(ws_narrs[23]['numObj'], 4)
        self.assertEqual(ws_narrs[23]['last_saved_at'],
                         datetime.datetime(2018, 1, 24, 19, 35, 30, 1000))

        # testing the broadest time range in the db
        earliest = 1468454614192
        latest = 1516822530001
        ws_narrs = self.dbi.list_ws_narratives(earliest, latest)
        self.assertEqual(len(ws_narrs), 24)

        # testing only given the lower bound
        ws_narrs = self.dbi.list_ws_narratives(minT=earliest)
        self.assertEqual(len(ws_narrs), 24)

        # testing only given the upper bound
        ws_narrs = self.dbi.list_ws_narratives(maxT=latest)
        self.assertEqual(len(ws_narrs), 24)

        # testing swap the lower/upper bounds
        ws_narrs = self.dbi.list_ws_narratives(minT=latest, maxT=earliest)
        self.assertEqual(len(ws_narrs), 24)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_ujs_results")
    def test_MetricsMongoDBs_list_ujs_results(self):
        minTime = 1500000932952
        maxTime = 1500046845591
        user_list1 = ['tgu2', 'umaganapathyswork', 'arfath']
        user_list2 = ['umaganapathyswork', 'arfath']

        epoch = datetime.datetime.utcfromtimestamp(0)
        # testing list_ujs_results return data, with userIds
        ujs = self.dbi.list_ujs_results(user_list1, minTime, maxTime)
        self.assertEqual(len(ujs), 14)
        ujs = self.dbi.list_ujs_results(user_list2, minTime, maxTime)
        self.assertEqual(len(ujs), 3)
        for uj in ujs:
            self.assertIn(uj.get('user'), user_list2)
            uj_creation_time = int((uj.get('created') -
                                    epoch).total_seconds() * 1000.0)
            self.assertTrue(minTime <= uj_creation_time <= maxTime)

        # testing list_ujs_results return data, without userIds
        ujs = self.dbi.list_ujs_results([], minTime, maxTime)
        self.assertEqual(len(ujs), 14)

        # testing list_ujs_results return data, different userIds and times
        ujs = self.dbi.list_ujs_results(['wjriehl'],
                                        1500052541065, 1500074641912)
        self.assertEqual(len(ujs), 8)
        ujs = self.dbi.list_ujs_results([], 1500052541065, 1500074641912)
        self.assertEqual(len(ujs), 14)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_constructor")
    def test_MetricsMongoDBController_constructor(self):
        cfg_arr = self.cfg
        # testing if all the required parameters are given
        with self.assertRaises(ValueError):
            try:
                cfg_arr = self.cfg
                db_ctr1 = MetricsMongoDBController(cfg_arr)
            except ValueError:
                pass
            else:
                raise ValueError
        # testing if all the required parameters are given, with an empty value
        with self.assertRaises(ValueError):
            try:
                cfg_arr = self.cfg
                cfg_arr['mongodb-host'] = ''
                db_ctr1 = MetricsMongoDBController(cfg_arr)
            except ValueError:
                pass
            else:
                raise ValueError
        # testing if any of the required parameters are missing
        for k in ['mongodb-host', 'mongodb-databases',
                  'mongodb-user', 'mongodb-pwd']:

            error_msg = '"{}" config variable must be defined '.format(k)
            error_msg += 'to start a MetricsMongoDBController!'

            cfg_arr = copy.deepcopy(self.cfg)
            cfg_arr.pop(k)
            with self.assertRaisesRegexp(ValueError, error_msg):
                db_ctr2 = MetricsMongoDBController(cfg_arr)

    # Uncomment to skip this test
    # @unittest.skip("skipped MetricsMongoDBController_config_str_to_list")
    def test_MetricsMongoDBController_config_str_to_list(self):
        # testing None config input
        user_list_str = None
        user_list = self.db_controller._config_str_to_list(user_list_str)
        self.assertFalse(len(user_list))

        # testing normal list
        user_list_str = 'user_1, user_2'
        user_list = self.db_controller._config_str_to_list(user_list_str)
        expected_list = ['user_1', 'user_2']
        self.assertEqual(len(user_list), 2)
        self.assertItemsEqual(user_list, expected_list)

        # testing list with spaces
        user_list_str = '  user_1, user_2    ,   , '
        user_list = self.db_controller._config_str_to_list(user_list_str)
        self.assertEqual(len(user_list), 2)
        self.assertItemsEqual(user_list, expected_list)

    # Uncomment to skip this test
    # @unittest.skip("skipped MetricsMongoDBController_process_parameters")
    def test_MetricsMongoDBController_process_parameters(self):

        # testing 'user_ids'
        user_list = ['user_1', 'user_2']
        params = {'user_ids': user_list}
        ret_params = self.db_controller._process_parameters(params)
        self.assertItemsEqual(ret_params.get('user_ids'), user_list)

        # no given 'user_ids'
        params = {}
        ret_params = self.db_controller._process_parameters(params)
        self.assertFalse(ret_params['user_ids'])

        # 'user_ids' is not a list
        params = {'user_ids': 'not_a_list_object'}
        with self.assertRaisesRegexp(ValueError,
                                     'Variable user_ids must be a list.'):
            self.db_controller._process_parameters(params)

        # testing removing 'kbasetest'
        user_list_kbasetest = ['user_1', 'user_2', 'kbasetest']
        params = {'user_ids': user_list_kbasetest}
        ret_params = self.db_controller._process_parameters(params)
        self.assertItemsEqual(ret_params.get('user_ids'), user_list)

        # testing epoch_range size 3
        params = {'epoch_range': (1, 2, 3)}
        with self.assertRaisesRegexp(ValueError,
                                     'Invalide epoch_range. Size must be 2.'):
            self.db_controller._process_parameters(params)

        # testing epoch_range
        params = {'epoch_range': ('2018-02-23T00:00:00+0000',
                                  '2018-02-25T00:00:00+0000')}
        ret_params = self.db_controller._process_parameters(params)
        self.assertEqual(ret_params.get('minTime'), 1519344000000)
        self.assertEqual(ret_params.get('maxTime'), 1519516800000)
        self.assertFalse(ret_params['user_ids'])

        date_time = datetime.datetime.strptime('2018-02-23T00:00:00+0000',
                                               '%Y-%m-%dT%H:%M:%S+0000')
        date = datetime.datetime.strptime('2018-02-25T00:00:00+0000',
                                          '%Y-%m-%dT%H:%M:%S+0000').date()
        params = {'epoch_range': (date_time, date)}
        ret_params = self.db_controller._process_parameters(params)
        self.assertEqual(ret_params.get('minTime'), 1519344000000)
        self.assertEqual(ret_params.get('maxTime'), 1519516800000)
        self.assertFalse(ret_params['user_ids'])

        params = {'epoch_range': ('2018-02-23T00:00:00+0000', '')}
        ret_params = self.db_controller._process_parameters(params)
        self.assertEqual(ret_params.get('minTime'), 1519344000000)
        self.assertEqual(ret_params.get('maxTime'), 1519516800000)
        self.assertFalse(ret_params['user_ids'])

        params = {'epoch_range': (None, '2018-02-25T00:00:00+0000')}
        ret_params = self.db_controller._process_parameters(params)
        self.assertEqual(ret_params.get('minTime'), 1519344000000)
        self.assertEqual(ret_params.get('maxTime'), 1519516800000)
        self.assertFalse(ret_params['user_ids'])

        # testing empty epoch_range
        params = {'epoch_range': (None, None)}
        ret_params = self.db_controller._process_parameters(params)
        today = datetime.date.today()
        minTime = ret_params.get('minTime')
        maxTime = ret_params.get('maxTime')
        minTime_from_today = (datetime.date(*time.localtime(minTime/1000.0)
                                            [:3]) - today).days
        maxTime_from_today = (datetime.date(*time.localtime(maxTime/1000.0)
                                            [:3]) - today).days
        self.assertEqual(minTime_from_today, -2)
        self.assertEqual(maxTime_from_today, 0)

        params = {}
        ret_params = self.db_controller._process_parameters(params)
        today = datetime.date.today()
        minTime = ret_params.get('minTime')
        maxTime = ret_params.get('maxTime')
        minTime_from_today = (datetime.date(*time.localtime(minTime/1000.0)
                                            [:3]) - today).days
        maxTime_from_today = (datetime.date(*time.localtime(maxTime/1000.0)
                                            [:3]) - today).days
        self.assertEqual(minTime_from_today, -2)
        self.assertEqual(maxTime_from_today, 0)

    # Uncomment to skip this test
    # @unittest.skip("test_db_controller_constructor")
    def test_db_controller_constructor(self):

        expected_admin_list = ['kkeller', 'scanon', 'psdehal', 'dolson',
                               'nlharris', 'dylan', 'chenry', 'ciservices',
                               'wjriehl', 'sychan', 'jjeffryes',
                               'thomasoniii', 'eapearson', 'qzhang', 'tgu2']
        self.assertItemsEqual(self.db_controller.adminList,
                              expected_admin_list)

        expected_metrics_admin_list = ['scanon', 'psdehal', 'dolson', 'chenry',
                                       'wjriehl', 'sychan', 'qzhang', 'tgu2',
                                       'eapearson']
        self.assertItemsEqual(self.db_controller.metricsAdmins,
                              expected_metrics_admin_list)

        expected_db_list = ['metrics', 'userjobstate', 'workspace',
                            'exec_engine', 'auth2']
        self.assertItemsEqual(self.db_controller.mongodb_dbList,
                              expected_db_list)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_db_controller_parse_app_id_method")
    def test_db_controller_parse_app_id_method(self):
        mthd = "kb_rnaseq_donwloader.export_rna_seq_expression_as_zip"
        exec_tasks = [{
            "job_input": {
                "method": mthd,
            }
        }, {
            "job_input": {
                "app_id": "kb_deseq/run_DESeq2",
                "method": "kb_deseq.run_deseq2_app",
            }
        }, {
            "job_input": {
                "app_id": "kb_cufflinks.run_Cuffdiff",
                "method": "kb_cufflinks/run_Cuffdiff",
            }
        }, {
            "job_input": {
                "app_id": "kb_deseq/run_DESeq2",
                "method": "kb_deseq/run_deseq2_app",
            }
        }, {
            "job_input": {
                "app_id": "kb_deseq.run_DESeq2",
                "method": "kb_deseq.run_deseq2_app",
            }
        }]

        # testing parse_app_id
        self.assertEqual(self.db_controller._parse_app_id(exec_tasks[0]), '')
        self.assertEqual(self.db_controller._parse_app_id(exec_tasks[1]),
                         'kb_deseq/run_DESeq2')
        self.assertEqual(self.db_controller._parse_app_id(exec_tasks[2]),
                         'kb_cufflinks/run_Cuffdiff')
        self.assertEqual(self.db_controller._parse_app_id(exec_tasks[3]),
                         'kb_deseq/run_DESeq2')
        self.assertEqual(self.db_controller._parse_app_id(exec_tasks[4]),
                         'kb_deseq/run_DESeq2')

        # testing parse_method
        self.assertEqual(self.db_controller._parse_method(exec_tasks[0]), mthd)
        self.assertEqual(self.db_controller._parse_method(exec_tasks[1]),
                         'kb_deseq.run_deseq2_app')
        self.assertEqual(self.db_controller._parse_method(exec_tasks[2]),
                         'kb_cufflinks.run_Cuffdiff')
        self.assertEqual(self.db_controller._parse_method(exec_tasks[3]),
                         'kb_deseq.run_deseq2_app')
        self.assertEqual(self.db_controller._parse_method(exec_tasks[4]),
                         'kb_deseq.run_deseq2_app')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_db_controller_map_narrative")
    def test_db_controller_map_narrative(self):
        wsids = ['15206', '23165', '27834']
        ws_narrs = self.dbi.list_ws_narratives()

        # test mapping with real data
        self.assertItemsEqual(self.db_controller._map_narrative(
            wsids[0], ws_narrs), ('', '0'))
        self.assertItemsEqual(self.db_controller._map_narrative(
            wsids[1], ws_narrs), ('', '0'))
        self.assertItemsEqual(self.db_controller._map_narrative(
            wsids[2], ws_narrs), ('Staging Test', '1'))

        # test mapping with fake data
        fake_wnarrs = [{
                    'meta': [{'k': 'narrative', 'v': '1'},
                             {'k': 'narrative_nice_name',
                              'v': 'fake_but_nice'}],
                    'name': 'joedoe:1468507681406',
                    'workspace_id': 15206
                }, {
                    'meta': [{'k': 'narrative_nice_name',
                              'v': 'nice_name_ver0'}],
                    'name': 'abc:1468512987188',
                    'workspace_id': 23165
                }, {
                    'meta': [{'k': 'narrative', 'v': '1'}],
                    'name': 'wo77:1468515770961',
                    'workspace_id': 27834}]
        self.assertItemsEqual(self.db_controller._map_narrative(
            wsids[0], fake_wnarrs), ('fake_but_nice', '1'))
        self.assertItemsEqual(self.db_controller._map_narrative(
            wsids[1], fake_wnarrs), ('nice_name_ver0', '0'))
        self.assertItemsEqual(self.db_controller._map_narrative(
            wsids[2], fake_wnarrs), ('wo77:1468515770961', '1'))

    # Uncomment to skip this test
    # @unittecst.skip("skipped test_db_controller_convert_isodate_to_millis")
    def test_db_controller_convert_isodate_to_millis(self):
        cdt1 = {'milis': 1500040533893,
                'date': datetime.datetime(2017, 7, 14, 13, 55, 33, 893000)}
        sdt1 = {'milis': 1500040545623,
                'date': datetime.datetime(2017, 7, 14, 13, 55, 45, 623000)}
        udt1 = {'milis': 1500040626665,
                'date': datetime.datetime(2017, 7, 14, 13, 57, 6, 665000)}
        cdt2 = {'milis': 1500040565733,
                'date': datetime.datetime(2017, 7, 14, 13, 56, 5, 733000)}
        sdt2 = {'milis': 1500040575585,
                'date': datetime.datetime(2017, 7, 14, 13, 56, 15, 585000)}
        udt2 = {'milis': 1500040661079,
                'date': datetime.datetime(2017, 7, 14, 13, 57, 41, 79000)}
        cdt3 = {'milis': 1500046845485,
                'date': datetime.datetime(2017, 7, 14, 15, 40, 45, 485000)}
        sdt3 = {'milis': 1500046850810,
                'date': datetime.datetime(2017, 7, 14, 15, 40, 50, 810000)}
        udt3 = {'milis':  1500047709785,
                'date': datetime.datetime(2017, 7, 14, 15, 55, 9, 785000)}
        input_data = [{
                'created': cdt1['date'],
                'started': sdt1['date'],
                'updated': udt1['date'],
                'user': 'user1'}, {
                'created': cdt2['date'],
                'started': sdt2['date'],
                'updated': udt2['date'],
                'user': 'user2'}, {
                'created': cdt3['date'],
                'started': sdt3['date'],
                'updated': udt3['date'],
                'user': 'user3'}]
        output_data = self.db_controller._convert_isodate_to_millis(
                           input_data, ['created', 'started', 'updated'])
        for dt in output_data:
            if dt['user'] == 'user1':
                self.assertEqual(dt['created'], cdt1['milis'])
                self.assertEqual(dt['started'], sdt1['milis'])
                self.assertEqual(dt['updated'], udt1['milis'])
            if dt['user'] == 'user2':
                self.assertEqual(dt['created'], cdt2['milis'])
                self.assertEqual(dt['started'], sdt2['milis'])
                self.assertEqual(dt['updated'], udt2['milis'])
            if dt['user'] == 'user3':
                self.assertEqual(dt['created'], cdt3['milis'])
                self.assertEqual(dt['started'], sdt3['milis'])
                self.assertEqual(dt['updated'], udt3['milis'])

        target_ret_data = [{
              'created': cdt1['milis'],
              'started': sdt1['milis'],
              'updated': udt1['milis'],
              'user': 'user1'}, {
              'created': cdt2['milis'],
              'started': sdt2['milis'],
              'updated': udt2['milis'],
              'user': 'user2'}, {
              'created': cdt3['milis'],
              'started': sdt3['milis'],
              'updated': udt3['milis'],
              'user': 'user3'}]
        # checking convertions
        self.assertItemsEqual(output_data, target_ret_data)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_join_task_ujs")
    def test_MetricsMongoDBController_join_task_ujs(self):
        # testing data sets
        mthd = "kb_rnaseq_donwloader.export_rna_seq_expression_as_zip"
        exec_tasks = [{
            "ujs_job_id": "5968cd75e4b08b65f9ff5d7c",
            "job_input": {
                "method": mthd,
            }
        }, {
            "ujs_job_id": "596832a4e4b08b65f9ff5d6f",
            "job_input": {
                "app_id": "kb_deseq/run_DESeq2",
                "method": "kb_deseq.run_deseq2_app",
                "params": [
                    {
                        "workspace_name": "tgu2:1481170361822"
                    }
                ],
                "wsid": 15206,
            }
        }, {
            "ujs_job_id": "5968e5fde4b08b65f9ff5d7d",
            "job_input": {
                "app_id": "kb_cufflinks/run_Cuffdiff",
                "method": "kb_cufflinks.run_Cuffdiff",
                "params": [
                    {
                        "workspace_name": ("umaganapathyswork:"
                                           "narrative_1498130853194")
                    }
                ],
                "wsid": 23165,
            }
        }]
        ujs_jobs = [{
            "_id": "596832a4e4b08b65f9ff5d6f",
            "user": "tgu2",
            "authstrat": "kbaseworkspace",
            "authparam": "15206",
            "created": 1500000932849,
            "updated": 1500001203182,
            "estcompl": None,
            "status": "done",
            "desc": "Execution engine job for kb_deseq.run_deseq2_app",
            "started": 1500000937695,
            "complete": True,
            "error": False
        }, {
            "_id": "5968cd75e4b08b65f9ff5d7c",
            "user": "arfath",
            "authstrat": "DEFAULT",
            "authparam": "DEFAULT",
            "created": 1500040565733,
            "updated": 1500040661079,
            "estcompl": None,
            "status": "done",
            "desc": "Execution engine job for " + mthd,
            "started": 1500040575585,
            "complete": True,
            "error": False
        }, {
            "_id": "5968e5fde4b08b65f9ff5d7d",
            "user": "umaganapathyswork",
            "authstrat": "kbaseworkspace",
            "authparam": "23165",
            "created": 1500046845485,
            "updated": 1500047709785,
            "estcompl": None,
            "status": "done",
            "desc": "Execution engine job for kb_cufflinks.run_Cuffdiff",
            "started": 1500046850810,
            "complete": True,
            "error": False
        }]
        # testing the correct data items appear in the joined result
        joined_results = self.db_controller._join_task_ujs(exec_tasks,
                                                           ujs_jobs)
        self.assertEqual(len(joined_results), 3)
        self.assertEqual(joined_results[0]['wsid'], '15206')
        self.assertEqual(joined_results[0]['app_id'], 'kb_deseq/run_DESeq2')
        self.assertEqual(joined_results[0]['method'],
                         'kb_deseq.run_deseq2_app')
        self.assertEqual(joined_results[0]['finish_time'], 1500001203182)
        self.assertIn('client_groups', joined_results[0])
        self.assertEqual(joined_results[0]['workspace_name'],
                         'tgu2:1481170361822')

        self.assertNotIn('wsid', joined_results[1])
        self.assertEqual(joined_results[1]['app_id'], mthd.replace('.', '/'))
        self.assertEqual(joined_results[1]['method'], mthd)
        self.assertEqual(joined_results[1]['finish_time'], 1500040661079)
        self.assertIn('client_groups', joined_results[1])
        self.assertNotIn('workspace_name', joined_results[1])

        self.assertEqual(joined_results[2]['wsid'], '23165')
        self.assertEqual(joined_results[2]['app_id'],
                         'kb_cufflinks/run_Cuffdiff')
        self.assertEqual(joined_results[2]['method'],
                         'kb_cufflinks.run_Cuffdiff')
        self.assertEqual(joined_results[2]['workspace_name'],
                         'umaganapathyswork:narrative_1498130853194')
        self.assertEqual(joined_results[2]['finish_time'], 1500047709785)
        self.assertIn('client_groups', joined_results[2])

    # Uncomment to skip this test
    # @unittest.skip("skipped_get_client_groups_from_cat")
    def test_MetricsMongoDBController_get_client_groups_from_cat(self):
        # testing if the data has expected structure
        clnt_ret = self.db_controller._get_client_groups_from_cat(
                self.getContext()['token'])
        self.assertIn('app_id', clnt_ret[0])
        self.assertIn('client_groups', clnt_ret[0])
        target_clnt = 'kb_upload'
        for clnt in clnt_ret:
            if target_clnt in clnt['app_id']:
                self.assertIn(target_clnt, clnt['client_groups'])

    # Uncomment to skip this test
    # @unittest.skip("skipped_get_activities_from_wsobjs")
    def test_MetricsMongoDBController_get_activities_from_wsobjs(self):
        start_datetime = datetime.datetime.strptime('2016-07-15T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')

        params = {'epoch_range': (start_datetime, end_datetime)}

        # testing if the data has expected structure
        act_ret = self.db_controller._get_activities_from_wsobjs(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        user_acts = act_ret['metrics_result']

        self.assertTrue(len(user_acts) == 11)
        self.assertEqual(user_acts[1]['_id']['username'], 'vkumar')
        self.assertEqual(user_acts[1]['_id']['ws_id'], 8768)
        self.assertEqual(user_acts[1]['_id']['year_mod'], 2016)
        self.assertEqual(user_acts[1]['_id']['month_mod'], 7)
        self.assertEqual(user_acts[1]['_id']['day_mod'], 15)
        self.assertEqual(user_acts[1]['obj_numModified'], 21)

    # Uncomment to skip this test
    # @unittest.skip("skipped get_narratives_from_wsobjs")
    def test_MetricsMongoDBController_get_narratives_from_wsobjs(self):
        start_datetime = datetime.datetime.strptime('2016-07-15T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')

        params = {'epoch_range': (start_datetime, end_datetime)}

        # testing if the data has expected structure
        narr_ret = self.db_controller._get_narratives_from_wsobjs(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        narrs = narr_ret['metrics_result']
        self.assertEqual(len(narrs), 2)
        self.assertEqual(narrs[1]['workspace_id'], 27834)
        self.assertEqual(narrs[1]['object_id'], 1)
        self.assertEqual(narrs[1]['object_version'], 11)
        self.assertEqual(narrs[1]['name'],
                         'psdehal:narrative_1513709108341')
        self.assertEqual(narrs[1]['nice_name'], 'Staging Test')
        self.assertEqual(narrs[1]['numObj'], 4)
        self.assertEqual(narrs[1]['last_saved_by'], 'psdehal')
        self.assertEqual(narrs[1]['last_saved_at'],
                         datetime.datetime(2018, 1, 24, 19, 35, 30, 1000))
        self.assertFalse(narrs[1]['deleted'])

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_update_user_info")
    def test_MetricsMongoDBController_update_user_info(self):
        user_list = ['sulbha', 'ytm123', 'xiaoli', 'andrew78', 'qzhang']
        start_datetime = datetime.datetime.strptime('2018-01-01T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')
        params = {'user_ids': user_list}
        params['epoch_range'] = (start_datetime, end_datetime)

        # testing update_user_info with given user_ids
        upd_ret = self.db_controller._update_user_info(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertEqual(upd_ret, 4)

        # testing update_user_info without user_ids
        params = {'user_ids': []}
        params['epoch_range'] = (start_datetime, end_datetime)
        upd_ret = self.db_controller._update_user_info(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertEqual(upd_ret, 37)

        # testing update_user_info with no match to update
        params = {'user_ids': ['abc1', 'abc2', 'abc3']}
        upd_ret = self.db_controller._update_user_info(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertEqual(upd_ret, 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped _update_daily_activities")
    def test_MetricsMongoDBController_update_daily_activities(self):
        start_datetime = datetime.datetime.strptime('2018-01-01T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')
        params = {'epoch_range': (start_datetime, end_datetime)}

        # testing update_daily_activities with given user_ids
        upd_ret = self.db_controller._update_daily_activities(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertEqual(upd_ret, 8)

        # testing update_daily_activities with no match to update
        start_datetime = datetime.datetime.strptime('2018-03-25T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')
        params = {'epoch_range': (start_datetime, end_datetime)}
        upd_ret = self.db_controller._update_daily_activities(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertEqual(upd_ret, 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped _update_narratives")
    def test_MetricsMongoDBController_update_narratives(self):
        start_datetime = datetime.datetime.strptime('2018-01-01T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')

        params = {'epoch_range': (start_datetime, end_datetime)}

        # ensure this record does not exist in the db yet
        n_cur = self.dbi.metricsDBs['metrics']['narratives'].find({
                    'workspace_id': 27834, 'object_id': 1,
                    'object_version': 11})
        self.assertEqual(len(list(n_cur)), 0)

        # testing update_narratives with given user_ids
        upd_ret = self.db_controller._update_narratives(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertEqual(upd_ret, 1)

        # confirm this record is upserted into the db
        n_cur = self.dbi.metricsDBs['metrics']['narratives'].find({
                    'workspace_id': 27834, 'object_id': 1,
                    'object_version': 11})
        n_list = list(n_cur)
        self.assertEqual(len(n_list), 1)
        self.assertEqual(n_list[0]['numObj'], 4)
        self.assertEqual(n_list[0]['name'], 'psdehal:narrative_1513709108341')
        self.assertEqual(n_list[0]['nice_name'], 'Staging Test')
        self.assertEqual(n_list[0]['last_saved_by'], 'psdehal')
        self.assertEqual(n_list[0]['last_saved_at'],
                         datetime.datetime(2018, 1, 24, 19, 35, 30, 1000))

        # testing 0 narrative updates
        start_dt = datetime.datetime.strptime('2018-03-25T00:00:00+0000',
                                              '%Y-%m-%dT%H:%M:%S+0000')
        end_dt = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                            '%Y-%m-%dT%H:%M:%S.%fZ')
        params = {'epoch_range': (start_dt, end_dt)}
        upd_ret1 = self.db_controller._update_narratives(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertEqual(upd_ret1, 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped MetricsMongoDBController_get_user_job_states")
    def test_MetricsMongoDBController_get_user_job_states(self):
        # testing if the data has expected structure and values
        user_list = ['tgu2', 'umaganapathyswork', 'arfath']
        params = {'user_ids': user_list}
        start_datetime = datetime.datetime.strptime('2017-07-14T02:55:32+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2017-07-14T16:08:53.956Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')
        params['epoch_range'] = (start_datetime, end_datetime)
        input_params = self.db_controller._process_parameters(params)
        self.assertEqual(input_params.get('minTime'), 1500000932000)
        self.assertEqual(input_params.get('maxTime'), 1500048533956)

        ujs_ret = self.db_controller.get_user_job_states(
                self.getContext()['user_id'],
                input_params, self.getContext()['token'])
        ujs = ujs_ret['job_states']
        self.assertEqual(len(ujs), 16)

        self.assertEqual(ujs[0]['wsid'], '15206')
        self.assertEqual(ujs[0]['app_id'], 'kb_deseq/run_DESeq2')
        self.assertEqual(ujs[0]['method'], 'kb_deseq.run_deseq2_app')
        self.assertEqual(ujs[0]['finish_time'], 1500001203182)
        self.assertIn('client_groups', ujs[0])
        self.assertIn('njs', ujs[0]['client_groups'])
        self.assertEqual(ujs[0]['workspace_name'], 'tgu2:1481170361822')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_get_user_details")
    def test_MetricsMongoDBController_get_user_details(self):
        user_list0 = []
        user_list = ['sulbha', 'ytm123', 'xiaoli', 'andrew78', 'qzhang']
        start_datetime = datetime.datetime.strptime('2018-01-01T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')

        # testing get_user_details return data with empty user_ids
        params = {'user_ids': user_list0}
        params['epoch_range'] = (start_datetime, end_datetime)

        users = self.db_controller.get_user_details(
                self.getContext()['user_id'],
                params, self.getContext()['token'])['metrics_result']
        self.assertEqual(len(users), 33)

        # testing get_user_details return data with specified user_ids
        params = {'user_ids': user_list}
        params['epoch_range'] = (start_datetime, end_datetime)

        users = self.db_controller.get_user_details(
                self.getContext()['user_id'],
                params, self.getContext()['token'])['metrics_result']

        self.assertEqual(len(users), 4)
        self.assertIn('username', users[0])
        self.assertIn('email', users[0])
        self.assertIn('full_name', users[0])
        self.assertIn('signup_at', users[0])
        self.assertIn('last_signin_at', users[0])
        self.assertIn('roles', users[0])

        self.assertEqual(users[1]['username'], 'xiaoli')
        self.assertEqual(users[1]['email'], 'csmbl2016@gmail.com')
        self.assertEqual(users[1]['full_name'], 'Xiaoli Sun')
        self.assertEqual(users[1]['signup_at'], 1518794641935)
        self.assertEqual(users[1]['last_signin_at'], 1518794641938)
        self.assertEqual(users[1]['roles'], [])

    # Uncomment to skip this test
    # @unittest.skip("skipped get_active_users_counts")
    def test_MetricsMongoDBController_get_active_users_counts(self):
        # testing get_active_users_counts return data
        params = {'epoch_range': (datetime.datetime(2018, 1, 1),
                                  datetime.datetime(2018, 3, 31))}

        # testing including kbstaff
        users = self.db_controller.get_active_users_counts(
                self.getContext()['user_id'], params,
                self.getContext()['token'], False)['metrics_result']
        self.assertEqual(len(users), 57)
        self.assertIn('numOfUsers', users[0])
        self.assertIn('yyyy-mm-dd', users[0])
        self.assertEqual(users[0]['yyyy-mm-dd'], '2018-1-1')
        self.assertEqual(users[0]['numOfUsers'], 1)
        self.assertEqual(users[1]['numOfUsers'], 4)
        self.assertEqual(users[2]['numOfUsers'], 6)
        self.assertEqual(users[3]['numOfUsers'], 8)

        # testing excluding kbstaff (by default) with reduced counts
        users = self.db_controller.get_active_users_counts(
                self.getContext()['user_id'], params,
                self.getContext()['token'])['metrics_result']
        self.assertEqual(len(users), 56)
        self.assertIn('numOfUsers', users[0])
        self.assertIn('yyyy-mm-dd', users[0])
        self.assertEqual(users[0]['yyyy-mm-dd'], '2018-1-1')
        self.assertEqual(users[0]['numOfUsers'], 1)
        self.assertEqual(users[1]['numOfUsers'], 1)
        self.assertEqual(users[2]['numOfUsers'], 2)
        self.assertEqual(users[3]['numOfUsers'], 2)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_get_app_metrics")
    def test_run_MetricsImpl_get_app_metrics(self):
        user_list = ['psdehal', 'umaganapathyswork', 'arfath']
        start_datetime = datetime.datetime.strptime('2017-07-14T02:55:32+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-01-24T19:35:24.247Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')
        m_params = {
            'user_ids': user_list,
            'epoch_range': (start_datetime, end_datetime)
        }
        # call your implementation
        ret = self.getImpl().get_app_metrics(self.getContext(), m_params)
        app_metrics_ret = ret[0]['job_states']
        self.assertEqual(len(app_metrics_ret), 5)

        # call implementation with only one user
        user_list = ['psdehal']
        m_params = {
            'user_ids': user_list,
            'epoch_range': (start_datetime, end_datetime)
        }
        ret = self.getImpl().get_app_metrics(self.getContext(), m_params)
        app_metrics_ret = ret[0]['job_states']
        self.assertEqual(len(app_metrics_ret), 1)
        self.assertIn(app_metrics_ret[0]['user'], user_list)
        self.assertEqual(app_metrics_ret[0]['wsid'], '27834')
        self.assertEqual(app_metrics_ret[0]['app_id'], 'kb_SPAdes/run_SPAdes')
        self.assertEqual(app_metrics_ret[0]['method'], 'kb_SPAdes.run_SPAdes')
        self.assertNotIn('finish_time', app_metrics_ret[0])
        self.assertIn('client_groups', app_metrics_ret[0])
        if 'ci' in self.cfg['kbase-endpoint']:
            self.assertIn('njs', app_metrics_ret[0]['client_groups'])
        else:
            self.assertIn('bigmemlong', app_metrics_ret[0]['client_groups'])
        self.assertEqual(app_metrics_ret[0]['narrative_name'], 'Staging Test')
        self.assertEqual(app_metrics_ret[0]['workspace_name'],
                         'psdehal:narrative_1513709108341')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_get_user_details")
    def test_run_MetricsImpl_get_user_details(self):
        # testing get_user_details return data with specified user_ids
        user_list = ['sulbha', 'ytm123', 'xiaoli', 'andrew78', 'qzhang']
        epoch_range = (datetime.datetime(2018, 1, 1),
                       datetime.datetime(2018, 3, 31))
        m_params = {
            'user_ids': user_list,
            'epoch_range': epoch_range
        }
        ret = self.getImpl().get_user_details(self.getContext(), m_params)
        users = ret[0]['metrics_result']
        self.assertEqual(len(users), 4)
        self.assertIn('username', users[0])
        self.assertIn('email', users[0])
        self.assertIn('full_name', users[0])
        self.assertIn('signup_at', users[0])
        self.assertIn('last_signin_at', users[0])
        self.assertIn('roles', users[0])

        # testing get_user_details return data with empty user_ids
        m_params = {
            'user_ids': [],
            'epoch_range': epoch_range
        }
        ret = self.getImpl().get_user_details(self.getContext(), m_params)
        users = ret[0]['metrics_result']
        self.assertEqual(len(users), 38)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_get_user_counts_per_day")
    def test_run_MetricsImpl_get_user_counts_per_day(self):
        m_params = {
            'epoch_range': (datetime.datetime(2018, 1, 1),
                            datetime.datetime(2018, 3, 31))
        }
        ret = self.getImpl().get_user_counts_per_day(
                                self.getContext(), m_params)
        # testing (excluding kbstaff by default)
        users = ret[0]['metrics_result']
        self.assertEqual(len(users), 58)
        self.assertIn('numOfUsers', users[0])
        self.assertIn('yyyy-mm-dd', users[0])
        self.assertEqual(users[0]['yyyy-mm-dd'], '2018-1-1')
        self.assertEqual(users[0]['numOfUsers'], 1)
        self.assertEqual(users[1]['numOfUsers'], 1)
        self.assertEqual(users[2]['numOfUsers'], 2)
        self.assertEqual(users[3]['numOfUsers'], 2)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_update_metrics")
    def test_run_MetricsImpl_update_metrics(self):
        m_params = {
            'user_ids': [],
            'epoch_range': (datetime.datetime(2018, 1, 1),
                            datetime.datetime(2018, 3, 31))
        }
        ret = self.getImpl().update_metrics(self.getContext(), m_params)
        upds = ret[0]['metrics_result']
        self.assertEqual(upds['user_updates'], 37)
        self.assertEqual(upds['activity_updates'], 8)
        self.assertEqual(upds['narrative_updates'], 1)
