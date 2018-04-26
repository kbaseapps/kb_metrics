# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
import datetime
import copy
from bson.objectid import ObjectId
from pymongo import MongoClient
from mock import patch
from os import environ
try:
    from ConfigParser import ConfigParser  # py2
except ImportError:
    from configparser import ConfigParser  # py3

from pymongo.errors import WriteError, ConfigurationError

from biokbase.workspace.client import Workspace as workspaceService
from kb_Metrics.kb_MetricsImpl import kb_Metrics
from kb_Metrics.kb_MetricsServer import MethodContext
from kb_Metrics.authclient import KBaseAuth as _KBaseAuth
from kb_Metrics.metricsdb_controller import MetricsMongoDBController
from kb_Metrics.metrics_dbi import MongoMetricsDBI


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

        cls.db_names = cls.client.database_names()
        for db in cls.db_names:
            if db != 'local':
                cls.client[db].command("createUser", "admin",
                                       pwd="password", roles=["readWrite"])

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

    def mock_MongoMetricsDBI(self, mongo_host, mongo_dbs,
                             mongo_user, mongo_psswd):
        self.mongo_clients = dict()
        self.metricsDBs = dict()
        for m_db in mongo_dbs:
            self.mongo_clients[m_db] = MongoClient()
            self.metricsDBs[m_db] = self.mongo_clients[m_db][m_db]

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_constructor")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_constructor(self):
        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # testing if the db is connected and handshakes cab be made
        exec_cur = dbi.metricsDBs['exec_engine']['exec_tasks'].find()
        self.assertEqual(len(list(exec_cur)), 84)
        ws_cur = dbi.metricsDBs['workspace']['workspaces'].find()
        self.assertEqual(len(list(ws_cur)), 28)
        wsobj_cur = dbi.metricsDBs['workspace']['workspaceObjects'].find()
        self.assertEqual(len(list(wsobj_cur)), 41)
        ujs_cur = dbi.metricsDBs['userjobstate']['jobstate'].find()
        self.assertEqual(len(list(ujs_cur)), 36)
        a_users_cur = dbi.metricsDBs['auth2']['users'].find()
        self.assertEqual(len(list(a_users_cur)), 37)
        m_users_cur = dbi.metricsDBs['metrics']['users'].find()
        self.assertTrue(len(list(m_users_cur)), 37)
        act_cur = dbi.metricsDBs['metrics']['daily_activities'].find()
        self.assertTrue(len(list(act_cur)) >= 1603)
        narrs_cur = dbi.metricsDBs['metrics']['narratives'].find()
        self.assertTrue(len(list(narrs_cur)), 10)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_exec_tasks")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_list_exec_tasks(self):
        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        min_time = 1500000932952
        max_time = 1500046845591

        # testing list_exec_tasks return data
        exec_tasks = dbi.list_exec_tasks(min_time, max_time)
        self.assertEqual(len(exec_tasks), 3)
        for tsk in exec_tasks:
            self.assertTrue(min_time <= tsk['creation_time'] <= max_time)
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
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_list_user_objects_from_wsobjs(self):
        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        min_time = 1468592344887
        max_time = 1519768865840
        ws_narrs = dbi.list_ws_narratives()
        ws_list = [wn['workspace_id'] for wn in ws_narrs]

        # test list_user_objects_from_wsobjs return count without wsid
        user_objs = dbi.list_user_objects_from_wsobjs(
                        min_time, max_time)
        self.assertEqual(len(user_objs), 37)

        # test list_user_objects_from_wsobjs returned values with wsid filter
        user_objs = dbi.list_user_objects_from_wsobjs(
                        min_time, max_time, ws_list)
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
    # @unittest.skip("skipped test_MetricsMongoDBs_list_kbstaff_usernames")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_list_kbstaff_usernames(self):
        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # check the returned data from dbi.list_kbstaff_usernames() vs db
        users_coll = self.client.metrics.users
        kbstaff_in_coll = list(users_coll.find(
                                {'kbase_staff': {'$in': [1, True]}},
                                {'username': 1, '_id': 0}))
        kbstaffList = dbi.list_kbstaff_usernames()
        self.assertItemsEqual(kbstaffList, kbstaff_in_coll)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_ws_owners")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_list_ws_owners(self):
        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # testing returned data
        ws_owners = dbi.list_ws_owners()
        self.assertEqual(len(ws_owners), 28)
        self.assertIn('ws_id', ws_owners[0])
        self.assertIn('username', ws_owners[0])
        self.assertIn('name', ws_owners[0])

        self.assertEqual(ws_owners[1]['ws_id'], 7645)
        self.assertIn(ws_owners[1]['username'], 'jplfaria')
        self.assertIn(ws_owners[1]['name'], 'jplfaria:1464632279763')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_aggr_user_details")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_aggr_user_details(self):
        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        min_time = 1516307704700
        max_time = 1520549345000
        user_list0 = []
        user_list = ['shahmaneshb', 'laramyenders', 'allmon', 'boris']

        # testing aggr_user_details_returned data structure
        users = dbi.aggr_user_details(user_list, min_time, max_time)
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

        users = dbi.aggr_user_details(user_list0, min_time, max_time)
        self.assertEqual(len(users), 37)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_aggr_unique_users_per_day")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_aggr_unique_users_per_day(self):
        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        min_time = 1514764800000
        max_time = 1522454400000

        # testing aggr_unique_users_per_day return data
        users = dbi.aggr_unique_users_per_day(min_time, max_time)
        self.assertEqual(len(users), 57)
        self.assertIn('numOfUsers', users[0])
        self.assertIn('yyyy-mm-dd', users[0])
        self.assertEqual(users[0]['yyyy-mm-dd'], '2018-1-1')
        self.assertEqual(users[0]['numOfUsers'], 1)
        self.assertEqual(users[1]['numOfUsers'], 4)
        self.assertEqual(users[2]['numOfUsers'], 6)
        self.assertEqual(users[3]['numOfUsers'], 8)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_update_user_records_WriteError")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    @patch('pymongo.collection.Collection.update_one')
    def test_MetricsMongoDBs_update_user_records_WriteError(self, mock_upd):
        err_msg = 'user write error thrown from mock'
        mock_upd.side_effect = WriteError(err_msg, 99999)

        dt1 = datetime.datetime(2018, 3, 12, 1, 13, 30)
        dt2 = datetime.datetime(2018, 3, 12, 1, 35, 30)
        upd_filter_set = {'username': 'test_u1', 'email': 'test_e1'}
        upd_data_set = {'full_name': 'test_nm1', 'roles': [],
                        'signup_at': dt1, 'last_signin_at': dt2}
        isKBstaff = False

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        with self.assertRaises(WriteError) as context_manager:
            dbi.update_user_records(upd_filter_set,
                                    upd_data_set, isKBstaff)
        self.assertEqual(err_msg, str(context_manager.exception.message))

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_update_user_records")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
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

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # testing freshly upserted result
        upd_ret = dbi.update_user_records(upd_filter_set,
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

        upd_ret = dbi.update_user_records(upd_filter_set,
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
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
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

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # testing freshly inserted result
        inst_ret = dbi.insert_activity_records(act_set)
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
        inst_ret1 = dbi.insert_activity_records(act_set1)
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
    # @unittest.skip("skipped test_update_activity_records_WriteError")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    @patch('pymongo.collection.Collection.update_one')
    def test_MetricsMongoDBs_update_activity_records_WriteError(
                                                self, mock_upd):
        err_msg = 'activity write error thrown from mock'
        mock_upd.side_effect = WriteError(err_msg, 99999)

        upd_filter_set = {'_id.username': 'joe',
                          '_id.year_mod': 2019,
                          '_id.month_mod': 1,
                          '_id.day_mod': 1,
                          '_id.ws_id': 20199991}
        upd_data_set = {'obj_numModified': 99}

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        with self.assertRaises(WriteError) as context_manager:
            dbi.update_activity_records(upd_filter_set, upd_data_set)
        self.assertEqual(err_msg, str(context_manager.exception.message))

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_update_activity_records")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
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

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # testing freshly upserted result
        upd_ret = dbi.update_activity_records(upd_filter_set1,
                                              upd_data_set1)
        self.assertTrue(upd_ret.raw_result.get('upserted'))
        self.assertFalse(upd_ret.raw_result.get('updatedExisting'))
        self.assertTrue(upd_ret.raw_result.get('ok'))
        self.assertEqual(upd_ret.raw_result.get('n'), 1)
        self.assertEqual(len(list(db_mda.find(upd_filter_set1))), 1)

        upd_ret = dbi.update_activity_records(upd_filter_set2,
                                              upd_data_set2)
        self.assertEqual(len(list(db_mda.find(upd_filter_set2))), 1)

        self.assertEqual(db_mda.find_one(
            upd_filter_set1)['obj_numModified'], 91)
        self.assertEqual(db_mda.find_one(
            upd_filter_set2)['obj_numModified'], 92)

        # testing updating existing record
        upd_ret = dbi.update_activity_records(upd_filter_set2,
                                              upd_data_set3)
        self.assertFalse(upd_ret.raw_result.get('upserted'))
        self.assertTrue(upd_ret.raw_result.get('updatedExisting'))
        self.assertTrue(upd_ret.raw_result.get('ok'))
        self.assertEqual(upd_ret.raw_result.get('n'), 1)
        self.assertEqual(db_mda.find_one(
            upd_filter_set2)['obj_numModified'], 93)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_update_narrative_records_WriteError")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    @patch('pymongo.collection.Collection.update_one')
    def test_MetricsMongoDBs_update_narrative_records_WriteError(self,
                                                                 mock_upd):
        err_msg = 'narrative write error thrown from mock'
        mock_upd.side_effect = WriteError(err_msg, 99999)

        upd_narr_filter = {'object_id': 99,
                           'object_version': 5,
                           'workspace_id': 30199990}
        upd_narr_data = {'last_saved_at': datetime.datetime(
            2019, 1, 24, 19, 35, 48, 1000), 'numObj': 7}

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        with self.assertRaises(WriteError) as context_manager:
            dbi.update_narrative_records(upd_narr_filter, upd_narr_data)
        self.assertEqual(err_msg, str(context_manager.exception.message))

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_update_narrative_records")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
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
            2019, 1, 24, 19, 35, 48, 1000), 'numObj': 7}

        db_mn = self.client.metrics.narratives
        assert db_mn.find_one(upd_filter_set1) is None
        assert db_mn.find_one(upd_filter_set2) is None

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # testing freshly upserted result
        upd_ret = dbi.update_narrative_records(upd_filter_set1,
                                               upd_data_set1)
        self.assertTrue(upd_ret.raw_result.get('upserted'))
        self.assertFalse(upd_ret.raw_result.get('updatedExisting'))
        self.assertTrue(upd_ret.raw_result.get('ok'))
        self.assertEqual(upd_ret.raw_result.get('n'), 1)
        self.assertEqual(len(list(db_mn.find(upd_filter_set1))), 1)

        upd_ret = dbi.update_narrative_records(upd_filter_set2,
                                               upd_data_set2)
        self.assertEqual(len(list(db_mn.find(upd_filter_set2))), 1)

        mnrecord = db_mn.find_one(upd_filter_set1)
        self.assertEqual(mnrecord['nice_name'], 'nice_to_have')
        self.assertEqual(mnrecord['numObj'], 3)

        mnrecord = db_mn.find_one(upd_filter_set2)
        self.assertEqual(mnrecord['nice_name'], 'nice_to_have_2')
        self.assertEqual(mnrecord['numObj'], 5)

        # testing updating existing record
        upd_ret = dbi.update_narrative_records(upd_filter_set2,
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
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_get_user_info(self):
        min_time = 1516307704700
        max_time = 1520549345000
        user_list0 = []
        user_list = ['shahmaneshb', 'laramyenders', 'allmon', 'boris']

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # testing get_user_info return data
        users = dbi.get_user_info(user_list0, min_time, max_time)
        self.assertEqual(len(users), 37)
        users = dbi.get_user_info(user_list, min_time, max_time)
        self.assertEqual(len(users), 4)
        self.assertIn('username', users[0])
        self.assertIn('email', users[0])
        self.assertIn('full_name', users[0])
        self.assertIn('signup_at', users[0])
        self.assertIn('last_signin_at', users[0])
        self.assertIn('roles', users[0])

    # Uncomment to skip this test
    # @unittest.skip("skipped MetricsMongoDBs_aggr_activities_from_wsobjs")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_aggr_activities_from_wsobjs(self):
        # testing aggr_activities_from_wsobjs return data
        min_time = 1468540813000
        max_time = 1519768865840

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        user_acts = dbi.aggr_activities_from_wsobjs(min_time, max_time)
        self.assertEqual(len(user_acts), 11)
        self.assertEqual(user_acts[4]['_id']['ws_id'], 29624)
        self.assertEqual(user_acts[4]['_id']['year_mod'], 2018)
        self.assertEqual(user_acts[4]['_id']['month_mod'], 2)
        self.assertEqual(user_acts[4]['_id']['day_mod'], 26)
        self.assertEqual(user_acts[4]['obj_numModified'], 5)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_ws_narratives")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_list_ws_narratives(self):
        min_time = 1468592344887
        max_time = 1519768865840

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # Testing with time limit
        ws_narrs = dbi.list_ws_narratives(min_time, max_time)
        self.assertEqual(len(ws_narrs), 12)

        # Testing ws/narratives with deleted ones
        ws_narrs = dbi.list_ws_narratives(include_del=True)
        self.assertEqual(len(ws_narrs), 28)

        # Testing without given time limit
        ws_narrs = dbi.list_ws_narratives()

        # Ensure 'narrative_nice_name' or 'narrative' exists in 'meta'
        for wn in ws_narrs:
            self.assertIn('narr_keys', wn)
            self.assertIn('narr_values', wn)
            self.assertTrue(any(d == 'narrative_nice_name'
                                or d == 'narrative'
                                for d in wn['narr_keys']))

        self.assertEqual(len(ws_narrs), 27)
        self.assertIn('username', ws_narrs[0])
        self.assertIn('workspace_id', ws_narrs[0])
        self.assertIn('name', ws_narrs[0])
        self.assertIn('deleted', ws_narrs[0])
        self.assertIn('desc', ws_narrs[0])
        self.assertIn('numObj', ws_narrs[0])
        self.assertIn('last_saved_at', ws_narrs[0])

        self.assertEqual(ws_narrs[23]['username'], 'vkumar')
        self.assertEqual(ws_narrs[23]['workspace_id'], 8781)
        self.assertEqual(ws_narrs[23]['name'],
                         'vkumar:1468639677500')
        self.assertEqual(ws_narrs[23]['narr_keys'][2], 'narrative')
        self.assertEqual(ws_narrs[23]['narr_values'][2], '45')
        self.assertFalse(ws_narrs[23]['deleted'])
        self.assertEqual(ws_narrs[23]['desc'], None)
        self.assertEqual(ws_narrs[23]['numObj'], 46)
        self.assertEqual(ws_narrs[23]['last_saved_at'],
                         datetime.datetime(2016, 7, 16, 4, 26, 20, 343000))

        # testing the broadest time range in the db
        earliest = 1468454614192
        latest = 1516822530001
        ws_narrs = dbi.list_ws_narratives(earliest, latest)
        self.assertEqual(len(ws_narrs), 27)

        # testing only given the lower bound
        ws_narrs = dbi.list_ws_narratives(minT=earliest)
        self.assertEqual(len(ws_narrs), 27)

        # testing only given the upper bound
        ws_narrs = dbi.list_ws_narratives(maxT=latest)
        self.assertEqual(len(ws_narrs), 27)

        # testing swap the lower/upper bounds
        ws_narrs = dbi.list_ws_narratives(minT=latest, maxT=earliest)
        self.assertEqual(len(ws_narrs), 27)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_ujs_results")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_list_ujs_results(self):
        min_time = 1500000932952
        max_time = 1500046845591
        user_list1 = ['tgu2', 'umaganapathyswork', 'arfath']
        user_list2 = ['umaganapathyswork', 'arfath']

        epoch = datetime.datetime.utcfromtimestamp(0)
        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # testing list_ujs_results return data, with userIds
        ujs = dbi.list_ujs_results(user_list1, min_time, max_time)
        self.assertEqual(len(ujs), 14)
        ujs = dbi.list_ujs_results(user_list2, min_time, max_time)
        self.assertEqual(len(ujs), 3)
        for uj in ujs:
            self.assertIn(uj.get('user'), user_list2)
            uj_creation_time = int((uj.get('created') -
                                    epoch).total_seconds() * 1000.0)
            self.assertTrue(min_time <= uj_creation_time <= max_time)

        # testing list_ujs_results return data, without userIds
        ujs = dbi.list_ujs_results([], min_time, max_time)
        self.assertEqual(len(ujs), 14)

        # testing list_ujs_results return data, different userIds and times
        ujs = dbi.list_ujs_results(['wjriehl'],
                                   1500052541065, 1500074641912)
        self.assertEqual(len(ujs), 8)
        ujs = dbi.list_ujs_results([], 1500052541065, 1500074641912)
        self.assertEqual(len(ujs), 14)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_narrative_info")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_list_narrative_info(self):
        o_list = ['fangfang', 'psdehal', 'jplfaria', 'pranjan77']
        ws_id_list = [8056, 8748, 1111]

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')

        # testing list_narrative_info return data, with and without filters
        narr_owners = dbi.list_narrative_info(ws_id_list, o_list)
        self.assertEqual(len(narr_owners), 1)
        self.assertIn(narr_owners[0]['ws'], ws_id_list)
        self.assertIn(narr_owners[0]['owner'], o_list)
        self.assertEqual(narr_owners[0]['name'], 'pranjan77:1466168703797')

        narr_owners = dbi.list_narrative_info(wsid_list=ws_id_list)
        self.assertEqual(len(narr_owners), 2)
        for nrr in narr_owners:
            self.assertIn(nrr['ws'], ws_id_list)
        self.assertEqual(narr_owners[0]['ws'], 8056)
        self.assertEqual(narr_owners[1]['ws'], 8748)
        self.assertEqual(narr_owners[0]['owner'], 'pranjan77')
        self.assertEqual(narr_owners[1]['owner'], 'eapearson')
        self.assertEqual(narr_owners[0]['name'], 'pranjan77:1466168703797')
        self.assertEqual(narr_owners[1]['name'], 'eapearson:1468518477765')

        narr_owners = dbi.list_narrative_info(owner_list=o_list)
        self.assertEqual(len(narr_owners), 10)
        for nrr in narr_owners:
            self.assertIn(nrr['owner'], o_list)
        self.assertEqual(narr_owners[9]['ws'], 27834)
        self.assertEqual(narr_owners[9]['owner'], 'psdehal')

        narr_owners = dbi.list_narrative_info()
        self.assertEqual(len(narr_owners), 24)

    # Uncomment to skip this test
    # @unittest.skip("skipped MetricsMongoDBs_list_ws_firstAccess")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBs_list_ws_firstAccess(self):
        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        min_time = 1468592344887
        max_time = 1519768865840
        ws_narrs = dbi.list_narrative_info()
        ws_list = [wn['ws'] for wn in ws_narrs]

        # test list_ws_firstAccess returned values with wsid filter
        ws_objs = dbi.list_ws_firstAccess(
                        min_time, max_time, ws_list)
        self.assertEqual(len(ws_objs), 2)
        for wobj in ws_objs:
            self.assertIn(wobj['ws'], ws_list)
        self.assertEqual(ws_objs[0]['ws'], 27834)
        self.assertEqual(ws_objs[1]['ws'], 8768)
        self.assertEqual(ws_objs[0]['yyyy-mm-dd'], '2017-12-21')
        self.assertEqual(ws_objs[1]['yyyy-mm-dd'], '2016-7-15')

        # test list_ws_firstAccess return count without wsid
        ws_objs = dbi.list_ws_firstAccess(
                        min_time, max_time)
        self.assertEqual(len(ws_objs), 9)
        for wobj in ws_objs:
            self.assertTrue('2016-7' < wobj['yyyy-mm-dd'] < '2018-3')
        self.assertEqual(ws_objs[0]['yyyy-mm-dd'], '2018-2-27')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_constructor")
    def test_MetricsMongoDBController_constructor(self):
        cfg_arr = self.cfg
        # testing if all the required parameters are given
        # if yes, no error is raised
        with self.assertRaises(ValueError):
            try:
                cfg_arr = self.cfg
                MetricsMongoDBController(cfg_arr)
            except ValueError:
                pass
            else:
                raise ValueError
        # testing if all the required parameters are given, with an empty host
        with self.assertRaises(ConfigurationError):
            try:
                cfg_arr = self.cfg
                cfg_arr['mongodb-host'] = ''
                MetricsMongoDBController(cfg_arr)
            except ConfigurationError as e:
                raise e
            else:
                pass
        # testing if any of the required parameters are missing
        for k in ['mongodb-host', 'mongodb-databases',
                  'mongodb-user', 'mongodb-pwd']:

            error_msg = '"{}" config variable must be defined '.format(k)
            error_msg += 'to start a MetricsMongoDBController!'

            cfg_arr = copy.deepcopy(self.cfg)
            cfg_arr.pop(k)
            with self.assertRaisesRegexp(ValueError, error_msg):
                MetricsMongoDBController(cfg_arr)

    # Uncomment to skip this test
    # @unittest.skip("test_MetricsMongoDBController_constructor2")
    def test_MetricsMongoDBController_constructor2(self):

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
        min_time = ret_params.get('minTime')
        max_time = ret_params.get('maxTime')
        min_time_from_today = (datetime.date(*time.localtime(min_time/1000.0)
                                             [:3]) - today).days
        max_time_from_today = (datetime.date(*time.localtime(max_time/1000.0)
                                             [:3]) - today).days
        self.assertEqual(min_time_from_today, -2)
        self.assertEqual(max_time_from_today, 0)

        params = {}
        ret_params = self.db_controller._process_parameters(params)
        today = datetime.date.today()
        min_time = ret_params.get('minTime')
        max_time = ret_params.get('maxTime')
        min_time_from_today = (datetime.date(*time.localtime(min_time/1000.0)
                                             [:3]) - today).days
        max_time_from_today = (datetime.date(*time.localtime(max_time/1000.0)
                                             [:3]) - today).days
        self.assertEqual(min_time_from_today, -2)
        self.assertEqual(max_time_from_today, 0)

    # Uncomment to skip this test
    # @unittest.skip("test _is_admin")
    def test_db_controller_is_admin(self):
        # testing user access permission
        not_permitted_u = 'user_joe'
        self.assertFalse(self.db_controller._is_admin(not_permitted_u))
        permitted_u = 'qzhang'
        self.assertTrue(self.db_controller._is_admin(permitted_u))

    # Uncomment to skip this test
    # @unittest.skip("test _is_metrics_admin")
    def test_db_controller_is_metrics_admin(self):
        # testing user access permission
        not_permitted_u = 'user_joe'
        self.assertFalse(self.db_controller._is_metrics_admin(not_permitted_u))
        permitted_u = 'qzhang'
        self.assertTrue(self.db_controller._is_metrics_admin(permitted_u))

    # Uncomment to skip this test
    # @unittest.skip("skipped test_db_controller_parse_app_id_method")
    def test_db_controller_parse_app_id_method(self):
        mthd = "kb_rnaseq_donwloader.export_rna_seq_expression_as_zip"
        exec_tasks = [{
            "job_input": {"method": mthd}
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
        self.assertEqual(self.db_controller._parse_app_id(
            exec_tasks[0]["job_input"]), '')
        self.assertEqual(self.db_controller._parse_app_id(
            exec_tasks[1]["job_input"]), 'kb_deseq/run_DESeq2')
        self.assertEqual(self.db_controller._parse_app_id(
            exec_tasks[2]["job_input"]), 'kb_cufflinks/run_Cuffdiff')
        self.assertEqual(self.db_controller._parse_app_id(
            exec_tasks[3]["job_input"]), 'kb_deseq/run_DESeq2')
        self.assertEqual(self.db_controller._parse_app_id(
            exec_tasks[4]["job_input"]), 'kb_deseq/run_DESeq2')

        # testing parse_method
        self.assertEqual(self.db_controller._parse_method(
            exec_tasks[0]["job_input"]), mthd)
        self.assertEqual(self.db_controller._parse_method(
            exec_tasks[1]["job_input"]), 'kb_deseq.run_deseq2_app')
        self.assertEqual(self.db_controller._parse_method(
            exec_tasks[2]["job_input"]), 'kb_cufflinks.run_Cuffdiff')
        self.assertEqual(self.db_controller._parse_method(
            exec_tasks[3]["job_input"]), 'kb_deseq.run_deseq2_app')
        self.assertEqual(self.db_controller._parse_method(
            exec_tasks[4]["job_input"]), 'kb_deseq.run_deseq2_app')

    # Uncomment to skip this test
    # @unittecst.skip("skipped test_db_controller_convert_isodate_to_milis")
    def test_db_controller_convert_isodate_to_milis(self):
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
        input_data = [{'created': cdt1['date'],
                       'started': sdt1['date'],
                       'updated': udt1['date'],
                       'user': 'user1'},
                      {
                          'created': cdt2['date'],
                          'started': sdt2['date'],
                          'updated': udt2['date'],
                          'user': 'user2'},
                      {
                          'created': cdt3['date'],
                          'started': sdt3['date'],
                          'updated': udt3['date'],
                          'user': 'user3'
                      }]
        output_data = self.db_controller._convert_isodate_to_milis(
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

        target_ret_data = [{'created': cdt1['milis'],
                            'started': sdt1['milis'],
                            'updated': udt1['milis'],
                            'user': 'user1'},
                           {
                            'created': cdt2['milis'],
                            'started': sdt2['milis'],
                            'updated': udt2['milis'],
                            'user': 'user2'},
                           {
                            'created': cdt3['milis'],
                            'started': sdt3['milis'],
                            'updated': udt3['milis'],
                            'user': 'user3'
                           }]
        # checking convertions
        self.assertItemsEqual(output_data, target_ret_data)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MTDBController_assemble_ujs_state")
    def test_MetricsMongoDBController_assemble_ujs_state(self):
        # testing data sets
        mthd = "kb_rnaseq_donwloader.export_rna_seq_expression_as_zip"
        exec_tasks = [{
            "ujs_job_id": "5968cd75e4b08b65f9ff5d7c",
            "job_input": {
                "method": mthd
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
                "params": [{"workspace_name":
                            ("umaganapathyswork:"
                             "narrative_1498130853194")}],
                "wsid": 23165,
            }
        }, {
            "ujs_job_id": "5968e5fde4b08b65f9ff3c4e",
            "job_input": {
                "app_id": "fake_mod/fake_app",
                "method": "fake_mod.fake_method",
                "params": [1230],
                "wsid": 8748,
            }
        }, {
            "ujs_job_id": "5968cd75e4b08b65f9ff5c7d",
            "job_input": {
                "method": mthd,
                "params": []
            }
        }, {
            "ujs_job_id": "564a4fd6e4b0d9c152289eac",
            "job_input": {
                "service_ver": "bace8ac842135ca4fa15b453f0e971b7293a09a9",
                "method": "KBaseRNASeq.getAlignmentStats",
                "params": [{
                    "output_obj_name": "wewe",
                    "ws_id": "srividya22:1447279981090",
                    "alignment_sample_id": "wewe"
                }]
            }
        }]

        ujs_jobs = [{
            "_id": ObjectId("596832a4e4b08b65f9ff5d6f"),
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
            "_id": ObjectId("5968cd75e4b08b65f9ff5d7c"),
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
            "_id": ObjectId("5968e5fde4b08b65f9ff5d7d"),
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
        }, {
            "_id": ObjectId("5968e5fde4b08b65f9ff3c4e"),
            "user": "JohnDoe",
            "authstrat": "DEFAULT",
            "authparam": "DEFAULT",
            "created": 1500040565733,
            "updated": 1500040661079,
            "estcompl": None,
            "status": "done",
            "started": 1500040575585,
            "complete": True,
            "error": False
        }, {
            "_id": ObjectId("5968cd75e4b08b65f9ff5c7d"),
            "user": "joesomeone",
            "authstrat": "kbaseworkspace",
            "authparam": "8056",
            "created": 1500046845485,
            "updated": 1500047709785,
            "estcompl": None,
            "status": "done",
            "desc": "",
            "started": 1500046850810,
            "complete": True,
            "error": False
        }, {
            "_id": ObjectId("564a4fd6e4b0d9c152289eac"),
            "user": "srividya22",
            "created": 1449032638757,
            "updated": 1449032658978,
            "estcompl": None,
            "service": "srividya22",
            "status": ("Error: Image was not found: "
                       "dockerhub-ci.kbase.us/kbasernaseq:"
                       "bace8ac842135ca4fa15b453f0e971b7293a09a9"),
            "desc": "AWE job for KBaseRNASeq.getAlignmentStats",
            "started": 1449032638823,
            "complete": True,
            "error": True,
            "authstrat": "DEFAULT",
            "authparam": "DEFAULT"
        }]

        # make sure the narratimve_name_map exists
        if self.db_controller.narrative_name_map == {}:
            self.db_controller.narrative_name_map = self.db_controller._get_narrative_name_map()
        # testing the correct data items appear in the assembled result
        joined_ujs0 = self.db_controller._assemble_ujs_state(ujs_jobs[0],
                                                             exec_tasks)
        self.assertEqual(joined_ujs0['wsid'], '15206')
        self.assertNotIn('narrative_name', joined_ujs0)
        self.assertNotIn('narrative_objNo', joined_ujs0)
        self.assertEqual(joined_ujs0['app_id'],
                         exec_tasks[1]['job_input']['app_id'])
        self.assertEqual(joined_ujs0['method'],
                         exec_tasks[1]['job_input']['method'])
        self.assertEqual(joined_ujs0['finish_time'], ujs_jobs[0]['updated'])
        self.assertIn('client_groups', joined_ujs0)
        self.assertEqual(joined_ujs0['workspace_name'],
                         exec_tasks[1]['job_input']['params'][0]
                         ['workspace_name'])

        joined_ujs1 = self.db_controller._assemble_ujs_state(ujs_jobs[1],
                                                             exec_tasks)
        self.assertNotIn('wsid', joined_ujs1)
        self.assertEqual(joined_ujs1['app_id'], mthd.replace('.', '/'))
        self.assertEqual(joined_ujs1['method'], mthd)
        self.assertEqual(joined_ujs1['finish_time'], ujs_jobs[1]['updated'])
        self.assertIn('client_groups', joined_ujs1)
        self.assertNotIn('workspace_name', joined_ujs1)

        joined_ujs2 = self.db_controller._assemble_ujs_state(ujs_jobs[2],
                                                             exec_tasks)
        self.assertEqual(joined_ujs2['wsid'], '23165')
        self.assertEqual(joined_ujs2['app_id'],
                         'kb_cufflinks/run_Cuffdiff')
        self.assertEqual(joined_ujs2['method'],
                         'kb_cufflinks.run_Cuffdiff')
        self.assertEqual(joined_ujs2['workspace_name'],
                         exec_tasks[2]['job_input']['params'][0]
                         ['workspace_name'])
        self.assertEqual(joined_ujs2['finish_time'],  ujs_jobs[2]['updated'])
        self.assertIn('client_groups', joined_ujs2)

        joined_ujs3 = self.db_controller._assemble_ujs_state(ujs_jobs[3],
                                                             exec_tasks)
        et_job_input = exec_tasks[3]["job_input"]
        self.assertEqual(joined_ujs3['wsid'], et_job_input['wsid'])
        self.assertEqual(joined_ujs3['narrative_name'], 'Method Cell Refactor - UI Fixes')
        self.assertEqual(joined_ujs3['narrative_objNo'], '94')
        self.assertEqual(joined_ujs3['app_id'], et_job_input['app_id'])
        self.assertEqual(joined_ujs3['method'], et_job_input['method'])
        self.assertEqual(joined_ujs3['finish_time'], ujs_jobs[3]['updated'])
        self.assertEqual(joined_ujs3['creation_time'], ujs_jobs[3]['created'])
        self.assertIn('client_groups', joined_ujs3)
        self.assertIn('workspace_name', joined_ujs3)

        joined_ujs4 = self.db_controller._assemble_ujs_state(ujs_jobs[4],
                                                             exec_tasks)
        self.assertEqual(joined_ujs4['wsid'], ujs_jobs[4]['authparam'])
        self.assertEqual(joined_ujs4['narrative_name'], 'outx')
        self.assertEqual(joined_ujs4['narrative_objNo'], '1')
        self.assertEqual(joined_ujs4['app_id'], mthd.replace('.', '/'))
        self.assertEqual(joined_ujs4['method'], mthd)
        self.assertEqual(joined_ujs4['finish_time'], ujs_jobs[4]['updated'])
        self.assertIn('client_groups', joined_ujs4)
        self.assertEqual(joined_ujs4['workspace_name'], 'pranjan77:1466168703797')

        joined_ujs5 = self.db_controller._assemble_ujs_state(ujs_jobs[5],
                                                             exec_tasks)
        et_job_input = exec_tasks[5]["job_input"]
        etj_params = et_job_input["params"][0]
        etj_methd = et_job_input["method"]
        self.assertEqual(joined_ujs5['wsid'], etj_params["ws_id"])
        self.assertEqual(joined_ujs5['narrative_name'], joined_ujs5['wsid'])
        self.assertEqual(joined_ujs5['narrative_objNo'], '1')
        self.assertEqual(joined_ujs5['app_id'], etj_methd.replace('.', '/'))
        self.assertEqual(joined_ujs5['method'], etj_methd)
        self.assertEqual(joined_ujs5['modification_time'], ujs_jobs[5]['updated'])
        self.assertIn('client_groups', joined_ujs5)
        self.assertEqual(joined_ujs5['workspace_name'], 'srividya22:1447279981090')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_join_task_ujs")
    def test_MetricsMongoDBController_join_task_ujs(self):
        # testing data sets
        mthd = "kb_rnaseq_donwloader.export_rna_seq_expression_as_zip"
        exec_tasks = [{
            "ujs_job_id": "5968cd75e4b08b65f9ff5d7c",
            "job_input": {
                "method": mthd
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
        self.assertEqual(joined_results[0]['app_id'],
                         exec_tasks[1]['job_input']['app_id'])
        self.assertEqual(joined_results[0]['method'],
                         exec_tasks[1]['job_input']['method'])
        self.assertEqual(joined_results[0]['finish_time'],
                         ujs_jobs[0]['updated'])
        self.assertIn('client_groups', joined_results[0])
        self.assertEqual(joined_results[0]['workspace_name'],
                         exec_tasks[1]['job_input']['params'][0]
                         ['workspace_name'])

        self.assertNotIn('wsid', joined_results[1])
        self.assertEqual(joined_results[1]['app_id'], mthd.replace('.', '/'))
        self.assertEqual(joined_results[1]['method'], mthd)
        self.assertEqual(joined_results[1]['finish_time'],
                         ujs_jobs[1]['updated'])
        self.assertIn('client_groups', joined_results[1])
        self.assertNotIn('workspace_name', joined_results[1])

        self.assertEqual(joined_results[2]['wsid'], '23165')
        self.assertEqual(joined_results[2]['app_id'],
                         'kb_cufflinks/run_Cuffdiff')
        self.assertEqual(joined_results[2]['method'],
                         'kb_cufflinks.run_Cuffdiff')
        self.assertEqual(joined_results[2]['workspace_name'],
                         'umaganapathyswork:narrative_1498130853194')
        self.assertEqual(joined_results[2]['finish_time'],
                         ujs_jobs[2]['updated'])
        self.assertIn('client_groups', joined_results[2])

    # Uncomment to skip this test
    # @unittest.skip("skipped_get_client_groups_from_cat")
    def test_db_ontroller_get_client_groups_from_cat(self):
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
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_db_controller_get_activities_from_wsobjs(self):
        start_datetime = datetime.datetime.strptime('2016-07-15T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')

        params = {'epoch_range': (start_datetime, end_datetime)}

        # testing if the data has expected structure
        act_ret = self.db_controller._get_activities_from_wsobjs(
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
        start_datetime = datetime.datetime.strptime(
            '2016-07-15T00:00:00+0000', '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime(
            '2018-03-31T00:00:10.000Z', '%Y-%m-%dT%H:%M:%S.%fZ')

        params = {'epoch_range': (start_datetime, end_datetime)}

        # testing if the data has expected structure
        narr_ret = self.db_controller._get_narratives_from_wsobjs(
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
    # @unittest.skip("skipped _get_narrative_name_map")
    def test_MetricsMongoDBController_get_narrative_name_map(self):
        # testing with local db data
        wnarr_map = self.db_controller._get_narrative_name_map()
        # print(wnarr_map)
        self.assertEqual(len(wnarr_map), 28)
        self.assertEqual(wnarr_map.get(8781),
                         ('vkumar:1468639677500', 'Ecoli refseq - July 15', '45'))
        self.assertEqual(wnarr_map.get(27834),
                         ('psdehal:narrative_1513709108341', 'Staging Test', '1'))
        self.assertEqual(wnarr_map.get(8736),
                         ('rsutormin:1468453294248', 'VisCellRefactor', '1'))
        self.assertEqual(wnarr_map.get(8748), ('eapearson:1468518477765',
                         'Method Cell Refactor - UI Fixes', '94'))
        self.assertTrue(wnarr_map.get(15206) is None)
        self.assertTrue(wnarr_map.get(23165) is None)

    # Uncomment to skip this test
    # @unittest.skip("skipped _map_ws_narr_names")
    def test_MetricsMongoDBController_map_ws_narr_names(self):
        w_nm, n_nm, n_ver = self.db_controller._map_ws_narr_names(8781)
        self.assertEqual(w_nm, 'vkumar:1468639677500')
        self.assertEqual(n_nm, 'Ecoli refseq - July 15')
        self.assertEqual(n_ver, '45')

        w_nm, n_nm, n_ver = self.db_controller._map_ws_narr_names(27834)
        self.assertEqual(w_nm, 'psdehal:narrative_1513709108341')
        self.assertEqual(n_nm, 'Staging Test')
        self.assertEqual(n_ver, '1')

        w_nm, n_nm, n_ver = self.db_controller._map_ws_narr_names(8736)
        self.assertEqual(w_nm, 'rsutormin:1468453294248')
        self.assertEqual(n_nm, 'VisCellRefactor')
        self.assertEqual(n_ver, '1')

        w_nm, n_nm, n_ver = self.db_controller._map_ws_narr_names(8748)
        self.assertEqual(w_nm, 'eapearson:1468518477765')
        self.assertEqual(n_nm, 'Method Cell Refactor - UI Fixes')
        self.assertEqual(n_ver, '94')

        w_nm, n_nm, n_ver = self.db_controller._map_ws_narr_names(15206)
        self.assertEqual(w_nm, '')
        self.assertEqual(n_nm, '')
        self.assertEqual(n_ver, '1')

        w_nm, n_nm, n_ver = self.db_controller._map_ws_narr_names(23165)
        self.assertEqual(w_nm, '')
        self.assertEqual(n_nm, '')
        self.assertEqual(n_ver, '1')

        w_nm, n_nm, n_ver = self.db_controller._map_ws_narr_names('qz:12345678')
        self.assertEqual(w_nm, 'qz:12345678')
        self.assertEqual(n_nm, 'qz:12345678')
        self.assertEqual(n_ver, '1')

    # Uncomment to skip this test
    # @unittest.skip("skipped _get_narrative_info")
    def test_MetricsMongoDBController_get_narrative_info(self):
        start_datetime = datetime.datetime.strptime(
            '2016-01-01T00:00:00+0000', '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime(
            '2018-04-30T00:00:10.000Z', '%Y-%m-%dT%H:%M:%S.%fZ')

        params = {'epoch_range': (start_datetime, end_datetime)}

        # testing with local db data
        narr_info = self.db_controller._get_narrative_info(params)
        self.assertEqual(len(narr_info), 2)
        self.assertEqual(narr_info[0]['owner'], 'vkumar')
        self.assertEqual(narr_info[1]['owner'], 'psdehal')
        self.assertEqual(narr_info[0]['ws'], 8768)
        self.assertEqual(narr_info[1]['ws'], 27834)
        self.assertEqual(narr_info[0]['name'], 'vkumar:1468592344827')
        self.assertEqual(
            narr_info[1]['name'],
            'psdehal:narrative_1513709108341')
        self.assertEqual(narr_info[0]['first_access'], '2016-7-15')
        self.assertEqual(narr_info[1]['first_access'], '2017-12-21')

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
            params, self.getContext()['token'])
        self.assertEqual(upd_ret, 4)

        # testing update_user_info without user_ids
        params = {'user_ids': []}
        params['epoch_range'] = (start_datetime, end_datetime)
        upd_ret = self.db_controller._update_user_info(
            params, self.getContext()['token'])
        self.assertEqual(upd_ret, 37)

        # testing update_user_info with no match to update
        params = {'user_ids': ['abc1', 'abc2', 'abc3']}
        upd_ret = self.db_controller._update_user_info(
            params, self.getContext()['token'])
        self.assertEqual(upd_ret, 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped_update_daily_activities")
    def test_db_controller_update_daily_activities(self):
        start_datetime = datetime.datetime.strptime('2018-01-01T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')
        params = {'epoch_range': (start_datetime, end_datetime)}

        # testing update_daily_activities with given user_ids
        upd_ret = self.db_controller._update_daily_activities(
            params, self.getContext()['token'])
        self.assertEqual(upd_ret, 8)

        # testing update_daily_activities with no match to update
        start_datetime = datetime.datetime.strptime('2018-03-25T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')
        params = {'epoch_range': (start_datetime, end_datetime)}
        upd_ret = self.db_controller._update_daily_activities(
            params, self.getContext()['token'])
        self.assertEqual(upd_ret, 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped_update_narratives")
    @patch.object(MongoMetricsDBI, '__init__', new=mock_MongoMetricsDBI)
    def test_MetricsMongoDBController_update_narratives(self):
        start_datetime = datetime.datetime.strptime('2018-01-01T00:00:00+0000',
                                                    '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')

        params = {'epoch_range': (start_datetime, end_datetime)}

        dbi = MongoMetricsDBI('', self.db_names, 'admin', 'password')
        # ensure this record does not exist in the db yet
        n_cur = dbi.metricsDBs['metrics']['narratives'].find({
            'workspace_id': 27834, 'object_id': 1,
            'object_version': 11})
        self.assertEqual(len(list(n_cur)), 0)

        # testing update_narratives with given user_ids
        upd_ret = self.db_controller._update_narratives(
            params, self.getContext()['token'])
        self.assertEqual(upd_ret, 1)

        # confirm this record is upserted into the db
        n_cur = dbi.metricsDBs['metrics']['narratives'].find({
            'workspace_id': 27834, 'object_id': 1,
            'object_version': 11})
        n_list = list(n_cur)
        self.assertEqual(len(n_list), 1)
        self.assertEqual(n_list[0]['numObj'], 4)
        self.assertEqual(n_list[0]['name'],
                         'psdehal:narrative_1513709108341')
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
            params, self.getContext()['token'])
        self.assertEqual(upd_ret1, 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped MetricsMongoDBController_get_user_job_states")
    def test_db_controller_get_user_job_states(self):
        # testing requesting user access permission
        requesting_user1 = 'qzhang'
        self.assertTrue(self.db_controller._is_admin(requesting_user1))

        requesting_user2 = 'JohnDoe'
        self.assertFalse(self.db_controller._is_admin(requesting_user2))

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

        # testing with the requesting user as an admin
        ujs_ret = self.db_controller.get_user_job_states(
            requesting_user1, input_params, self.getContext()['token'])
        ujs = ujs_ret['job_states']
        self.assertEqual(len(ujs), 16)

        self.assertEqual(ujs[0]['wsid'], '15206')
        self.assertEqual(ujs[0]['app_id'], 'kb_deseq/run_DESeq2')
        self.assertEqual(ujs[0]['method'], 'kb_deseq.run_deseq2_app')
        self.assertEqual(ujs[0]['finish_time'], 1500001203182)
        self.assertIn('client_groups', ujs[0])
        self.assertIn('njs', ujs[0]['client_groups'])
        self.assertEqual(ujs[0]['workspace_name'], 'tgu2:1481170361822')
        self.assertEqual(params['user_ids'], user_list)

        # testing the requesting user is not an admin and with no data
        ujs_ret = self.db_controller.get_user_job_states(
            requesting_user2, input_params, self.getContext()['token'])
        ujs = ujs_ret['job_states']
        self.assertEqual(len(ujs), 0)
        self.assertEqual(params['user_ids'], [requesting_user2])

        # testing the requesting user is not an admin but has job(s)
        mthd = "kb_rnaseq_donwloader.export_rna_seq_expression_as_zip"
        requesting_user3 = 'arfath'
        self.assertFalse(self.db_controller._is_admin(requesting_user3))
        ujs_ret = self.db_controller.get_user_job_states(
            requesting_user3, input_params, self.getContext()['token'])
        ujs = ujs_ret['job_states']
        self.assertEqual(len(ujs), 2)
        self.assertFalse(ujs[0].get('wsid'))
        self.assertEqual(ujs[0]['app_id'], mthd.replace('.', '/'))
        self.assertEqual(ujs[0]['method'], mthd)
        self.assertEqual(ujs[0]['finish_time'], 1500040626665)
        self.assertIn('client_groups', ujs[0])
        self.assertIn('njs', ujs[0]['client_groups'])
        self.assertFalse(ujs[0].get('workspace_name'))
        self.assertEqual(ujs[0]['user'], requesting_user3)
        self.assertEqual(params['user_ids'], [requesting_user3])

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
        self.assertEqual(len(users), 57)
        self.assertIn('numOfUsers', users[0])
        self.assertIn('yyyy-mm-dd', users[0])
        self.assertEqual(users[0]['yyyy-mm-dd'], '2018-1-1')
        self.assertEqual(users[0]['numOfUsers'], 1)
        self.assertEqual(users[1]['numOfUsers'], 4)
        self.assertEqual(users[2]['numOfUsers'], 6)
        self.assertEqual(users[3]['numOfUsers'], 8)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsMongoDBController_get_narrative_stats")
    def test_run_MetricsMongoDBController_get_narrative_stats(self):
        m_params = {
            'epoch_range': (datetime.datetime(2016, 1, 1),
                            datetime.datetime(2018, 4, 30))
        }
        # testing with given parameter values
        narr_stats = self.db_controller.get_narrative_stats(
            self.getContext()['user_id'], m_params,
            self.getContext()['token'])['metrics_result']
        self.assertEqual(len(narr_stats), 2)
        self.assertEqual(narr_stats[0]['owner'], 'vkumar')
        self.assertEqual(narr_stats[1]['owner'], 'psdehal')
        self.assertEqual(narr_stats[0]['ws'], 8768)
        self.assertEqual(narr_stats[1]['ws'], 27834)
        self.assertEqual(narr_stats[0]['name'], 'vkumar:1468592344827')
        self.assertEqual(
            narr_stats[1]['name'],
            'psdehal:narrative_1513709108341')
        self.assertEqual(narr_stats[0]['first_access'], '2016-7-15')
        self.assertEqual(narr_stats[1]['first_access'], '2017-12-21')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_get_total_logins")
    def test_run_MetricsImpl_get_total_logins(self):

        # with time range when there are login records
        m_params = {
            'epoch_range': (datetime.datetime(2016, 1, 1),
                            datetime.datetime(2018, 4, 30))}
        ret = self.getImpl().get_total_logins(self.getContext(), m_params)
        tot_logins = ret[0]['metrics_result']
        self.assertEqual(len(tot_logins), 2)
        for tl in tot_logins:
            if tl['_id'] == {'year': 2016, 'month': 7}:
                self.assertEqual(tl['year_mon_total_logins'], 26)
            if tl['_id'] == {'year': 2018, 'month': 1}:
                self.assertEqual(tl['year_mon_total_logins'], 2)

        # with time range when there is fewer login records
        m_params = {'epoch_range': (1420083768000, 1505876263000)}
        ret = self.getImpl().get_total_logins(self.getContext(), m_params)
        tot_logins = ret[0]['metrics_result']
        self.assertEqual(len(tot_logins), 1)
        self.assertEqual(tot_logins[0]['_id'], {'year': 2016, 'month': 7})
        self.assertEqual(tot_logins[0]['year_mon_total_logins'], 26)

        # with time range when there is no login records
        m_params = {
            'epoch_range': (datetime.datetime(2016, 9, 30),
                            datetime.datetime(2017, 6, 30))}
        ret = self.getImpl().get_total_logins(self.getContext(), m_params)
        tot_logins = ret[0]['metrics_result']
        self.assertEqual(len(tot_logins), 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_get_narrative_stats")
    def test_run_MetricsImpl_get_narrative_stats(self):
        m_params = {
            'epoch_range': (datetime.datetime(2016, 1, 1),
                            datetime.datetime(2018, 4, 30))
        }
        # testing with given parameter values
        ret = self.getImpl().get_narrative_stats(self.getContext(), m_params)
        narr_stats = ret[0]['metrics_result']
        print(narr_stats)
        self.assertEqual(len(narr_stats), 2)

        # assertItemsEqual only checks the keys
        self.assertItemsEqual(narr_stats[0],
                              {'owner': 'mary', 'ws': 321,
                               'name': 'mary:1468592344827',
                               'first_access': u'yyyy-mm-dd'})
        self.assertItemsEqual(narr_stats[1],
                              {'owner': 'john', 'ws': 123,
                               'name': 'john:narrative_1513709108341',
                               'first_access': u'yyyy-mm-dd'})
        self.assertEqual(narr_stats[0]['owner'], 'vkumar')
        self.assertEqual(narr_stats[1]['owner'], 'psdehal')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_get_user_logins")
    def test_run_MetricsImpl_get_user_logins(self):
        m_params = {
            'epoch_range': (datetime.datetime(2016, 1, 1),
                            datetime.datetime(2018, 4, 30))}
        # with time range when there are login records
        ret = self.getImpl().get_user_logins(self.getContext(), m_params)
        usr_logins = ret[0]['metrics_result']
        self.assertEqual(len(usr_logins), 14)
        self.assertItemsEqual(usr_logins[0],
                              {'_id': {'username': 'eapearson',
                                       'year': 2016, 'month': 7},
                               'year_mon_user_logins': 3})
        self.assertItemsEqual(usr_logins[1],
                              {'_id': {'username': 'fangfang',
                                       'year': 2016, 'month': 7},
                               'year_mon_user_logins': 2})

        self.assertEqual(usr_logins[3]['_id'],
                         {'username': 'joedoe', 'year': 2018, 'month': 1})
        self.assertEqual(usr_logins[3]['year_mon_user_logins'], 1)

        self.assertEqual(usr_logins[7]['_id'],
                         {'username': 'psdehal',
                          'year': 2018, 'month': 1})
        self.assertEqual(usr_logins[7]['year_mon_user_logins'], 1)

        self.assertEqual(usr_logins[13]['_id'],
                         {'username': 'wjriehl',
                          'year': 2016, 'month': 7})
        self.assertEqual(usr_logins[13]['year_mon_user_logins'], 3)

        # with time range when there are fewer user login records
        m_params = {'epoch_range': (1420083768000, 1505876263000)}
        ret = self.getImpl().get_user_logins(self.getContext(), m_params)
        usr_logins = ret[0]['metrics_result']
        self.assertEqual(len(usr_logins), 12)
        self.assertItemsEqual(usr_logins[3],
                              {'_id': {'username': 'jplfaria',
                                       'year': 2016, u'month': 7},
                              'year_mon_user_logins': 1})
        self.assertItemsEqual(usr_logins[6],
                              {'_id': {'username': 'rsutormin',
                                       'year': 2016, u'month': 7},
                              'year_mon_user_logins': 1})

        # with time range when there are even fewer user login records
        m_params = {
            'epoch_range': (datetime.datetime(2017, 9, 30),
                            datetime.datetime(2018, 4, 30))}
        ret = self.getImpl().get_user_logins(self.getContext(), m_params)
        usr_logins = ret[0]['metrics_result']
        self.assertEqual(len(usr_logins), 2)
        self.assertItemsEqual(usr_logins[0],
                              {'_id': {'username': 'joedoe',
                                       'year': 2018, 'month': 1},
                               'year_mon_user_logins': 1})
        self.assertItemsEqual(usr_logins[1],
                              {'_id': {'username': 'psdehal',
                                       'year': 2018, 'month': 1},
                               'year_mon_user_logins': 1})

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_get_user_numObjs")
    def test_run_get_user_numObjs(self):
        m_params = {
            'epoch_range': (datetime.datetime(2016, 1, 1),
                            datetime.datetime(2018, 4, 30))}
        # Second, call your implementation
        ret = self.getImpl().get_user_numObjs(self.getContext(), m_params)
        usr_objNum = ret[0]['metrics_result']
        self.assertEqual(len(usr_objNum), 14)
        self.assertItemsEqual(usr_objNum[0],
                              {'_id': {'username': 'eapearson',
                                       'year': 2016, 'month': 7},
                               'count_user_numObjs': 258})
        self.assertItemsEqual(usr_objNum[1],
                              {'_id': {'username': 'fangfang',
                                       'year': 2016, 'month': 7},
                               'count_user_numObjs': 2})
        self.assertItemsEqual(usr_objNum[3],
                              {'_id': {'username': 'joedoe',
                                       'year': 2018, 'month': 1},
                               'count_user_numObjs': 100})
        self.assertItemsEqual(usr_objNum[6],
                              {'_id': {'username': 'pranjan77',
                                       'year': 2016, 'month': 7},
                               'count_user_numObjs': 124})
        self.assertItemsEqual(usr_objNum[7],
                              {'_id': {'username': 'psdehal',
                                       'year': 2018, 'month': 1},
                               'count_user_numObjs': 4})
        self.assertItemsEqual(usr_objNum[13],
                              {'_id': {'username': 'wjriehl',
                                       'year': 2016, 'month': 7},
                               'count_user_numObjs': 48})

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
        self.assertEqual(len(users), 59)
        self.assertIn('numOfUsers', users[0])
        self.assertIn('yyyy-mm-dd', users[0])
        self.assertEqual(users[0]['yyyy-mm-dd'], '2018-1-1')
        self.assertEqual(users[0]['numOfUsers'], 1)
        self.assertEqual(users[1]['numOfUsers'], 4)
        self.assertEqual(users[2]['numOfUsers'], 6)
        self.assertEqual(users[3]['numOfUsers'], 8)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_update_metrics")
    def test_run_MetricsImpl_update_metrics(self):
        m_params = {
            'user_ids': [],
            'epoch_range': (datetime.datetime(2018, 1, 1),
                            datetime.datetime(2018, 3, 31))
        }

        # testing user access permission
        err_msg = 'You do not have permission to invoke this action.'
        not_permitted_u = 'user_joe'
        with self.assertRaisesRegexp(ValueError, err_msg):
            self.db_controller.update_metrics(
                not_permitted_u, m_params, self.getContext()['token'])

        ret = self.getImpl().update_metrics(self.getContext(), m_params)
        upds = ret[0]['metrics_result']
        self.assertEqual(upds['user_updates'], 37)
        self.assertEqual(upds['activity_updates'], 8)
        self.assertEqual(upds['narrative_updates'], 1)
