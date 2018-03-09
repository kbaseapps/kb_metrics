# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
import datetime
from pymongo import MongoClient

from os import environ
try:
    from ConfigParser import ConfigParser  # py2
except:
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

        cls.init_mongodb()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    @classmethod
    def init_mongodb(cls):
        print ('starting to build local mongoDB')

        os.system("sudo service mongodb start")
        os.system("mongod --version")
        os.system("cat /var/log/mongodb/mongodb.log | grep 'waiting for connections on port 27017'")

        client = MongoClient(port=27017)
        cls._insert_data(client, 'workspace', 'workspaces')
        cls._insert_data(client, 'exec_engine', 'exec_tasks')
        cls._insert_data(client, 'userjobstate', 'jobstate')
        cls._insert_data(client, 'workspace', 'workspaceObjects')
        cls._insert_data(client, 'auth2', 'users')
        cls._insert_data(client, 'metrics', 'users')
        cls._insert_data(client, 'metrics', 'daily_activities')

        cls.db_names = client.database_names()
        # updating created to timstamp field for userjobstate.jobstate
        for jrecord in client.userjobstate.jobstate.find():
            created_str = jrecord.get('created')
            client.userjobstate.jobstate.update_many(
                {"created": created_str},
                {"$set": {"created": datetime.datetime.utcfromtimestamp(int(created_str) / 1000)}}
            )

        for db in client.database_names():
            if db != 'local':
                client[db].command("createUser", "admin", pwd="password", roles=["readWrite"])

        cls.dbi =  MongoMetricsDBI('', client.database_names(), 'admin', 'password')

    @classmethod
    def _insert_data(cls, client, db_name, table):

        db = client[db_name]

        record_file = os.path.join('db_files', 'ci_{}.{}.json'.format(db_name, table))
        json_data = open(record_file).read()
        records = json.loads(json_data)

        db[table].drop()
        db[table].insert_many(records)
        print ('Inserted {} records for {}.{}'.format(len(records), db_name, table))

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
        self.assertEqual(len(list(wsobj_cur)), 15)
        ujs_cur = self.dbi.metricsDBs['userjobstate']['jobstate'].find()
        self.assertEqual(len(list(ujs_cur)), 36)
        act_cur = self.dbi.metricsDBs['metrics']['daily_activities'].find()
        self.assertEqual(len(list(act_cur)), 1603)

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
        self.assertEqual(exec_tasks[0]['ujs_job_id'], '596832a4e4b08b65f9ff5d6f')
        self.assertEqual(exec_tasks[0]['job_input']['wsid'], 15206)
        self.assertEqual(exec_tasks[1]['ujs_job_id'], '5968cd75e4b08b65f9ff5d7c')
        self.assertNotIn('wsid', exec_tasks[1]['job_input'])
        self.assertEqual(exec_tasks[2]['ujs_job_id'], '5968e5fde4b08b65f9ff5d7d')
        self.assertEqual(exec_tasks[2]['job_input']['wsid'], 23165)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_user_objects_from_wsobjs")
    def test_MetricsMongoDBs_list_user_objects_from_wsobjs(self):
        db_coll1 = self.dbi.metricsDBs['workspace']['workspaceObjects']
        for wrecord in db_coll1.find():
            moddate_str = wrecord.get('moddate')
            if type(moddate_str) not in [datetime.date, datetime.datetime]:
                db_coll1.update_many(
                    {"moddate": moddate_str},
                    {"$set": {"moddate": datetime.datetime.utcfromtimestamp(int(moddate_str) / 1000)}}
                )

        minTime = 1519668635550
        maxTime = 1519768865840

        # testing list_user_objects_from_wsobjs return data
        user_objs = self.dbi.list_user_objects_from_wsobjs(minTime, maxTime)
        self.assertEqual(len(user_objs), 15)

        self.assertIn('workspace_id', user_objs[0])
        self.assertIn('object_id', user_objs[0])
        self.assertIn('object_name', user_objs[0])
        self.assertIn('object_version', user_objs[0])
        self.assertIn('moddate', user_objs[0])
        self.assertIn('deleted', user_objs[0])

        self.assertEqual(user_objs[1]['workspace_id'], 29624)
        self.assertEqual(user_objs[1]['object_id'], 2)
        self.assertEqual(user_objs[1]['object_name'],
                'rhodobacter_CACIA14H1.reference')
        self.assertEqual(user_objs[1]['object_version'], 8)
        self.assertEqual(user_objs[1]['moddate'],
                datetime.datetime(2018, 2, 26, 22, 42, 20))
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
        db_coll2 = self.dbi.metricsDBs['auth2']['users']
        for urecord in db_coll2.find():
            create_str = urecord.get('create')
            login_str = urecord.get('login')
            if type(create_str) not in [datetime.date, datetime.datetime]:
                db_coll2.update_many(
                    {"create": create_str, "login": login_str},
                    {"$set": {"create": datetime.datetime.utcfromtimestamp(int(create_str) / 1000),
                              "login": datetime.datetime.utcfromtimestamp(int(login_str) / 1000)}}
                )

        minTime = 1516307704700
        maxTime = 1520549345000
        user_list0 = []
        user_list = ['shahmaneshb', 'laramyenders', 'allmon', 'boris']

        # testing aggr_user_details_returneddata structure
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
						datetime.datetime(2018, 2, 16, 15, 19, 55))
        self.assertEqual(users[1]['last_signin_at'],
						datetime.datetime(2018, 2, 16, 15, 19, 56))
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
        self.assertEqual(len(users), 57)
        self.assertIn('numOfUsers', users[0])
        self.assertIn('yyyy-mm-dd', users[0])
        self.assertEqual(users[0]['yyyy-mm-dd'], '2018-1-1')
        self.assertEqual(users[0]['numOfUsers'], 1)
        self.assertEqual(users[1]['numOfUsers'], 4)
        self.assertEqual(users[2]['numOfUsers'], 6)
        self.assertEqual(users[3]['numOfUsers'], 8)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_get_user_info")
    def test_MetricsMongoDBs_get_user_info(self):
        minTime = 1516307704700
        maxTime = 1520549345000
        user_list0 = []
        user_list = ['shahmaneshb', 'laramyenders', 'allmon', 'boris']

        # testing get_user_info return data
        users = self.dbi.get_user_info(user_list0, minTime, maxTime)
        self.assertEqual(len(users), 33)
        users = self.dbi.get_user_info(user_list, minTime, maxTime)
        self.assertEqual(len(users), 3)
        self.assertIn('username', users[0])
        self.assertIn('email', users[0])
        self.assertIn('full_name', users[0])
        self.assertIn('signup_at', users[0])
        self.assertIn('last_signin_at', users[0])
        self.assertIn('roles', users[0])

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_aggr_activities_from_wsobjs")
    def test_MetricsMongoDBs_aggr_activities_from_wsobjs(self):
        db_coll1 = self.dbi.metricsDBs['workspace']['workspaceObjects']
        for wrecord in db_coll1.find():
            moddate_str = wrecord.get('moddate')
            if type(moddate_str) not in [datetime.date, datetime.datetime]:
                db_coll1.update_many(
                    {"moddate": moddate_str},
                    {"$set": {"moddate": datetime.datetime.utcfromtimestamp(int(moddate_str) / 1000)}}
                )

        # testing aggr_activities_from_wsobjs return data
        minTime = 1519668635550
        maxTime = 1519768865840

        user_acts = self.dbi.aggr_activities_from_wsobjs(minTime, maxTime)
        self.assertTrue(len(user_acts) == 7)
        self.assertIn('ws_id', user_acts[0]['_id'])
        self.assertIn('year_mod', user_acts[0]['_id'])
        self.assertIn('month_mod', user_acts[0]['_id'])
        self.assertIn('day_mod', user_acts[0]['_id'])
        self.assertIn('obj_numModified', user_acts[0])

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBs_list_ws_narratives")
    def test_MetricsMongoDBs_list_ws_narratives(self):
        ws_narrs = self.dbi.list_ws_narratives()
        self.assertEqual(len(ws_narrs), 27)
        self.assertIn('username', ws_narrs[0])
        self.assertIn('workspace_id', ws_narrs[0])
        self.assertIn('name', ws_narrs[0])
        self.assertIn('meta', ws_narrs[0])
        self.assertIn('deleted', ws_narrs[0])
        self.assertIn('desc', ws_narrs[0])
        self.assertIn('numObj', ws_narrs[0])
        self.assertIn('last_saved_at', ws_narrs[0])

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
        self.assertEqual(len(ujs), 15)
        ujs = self.dbi.list_ujs_results(user_list2, minTime, maxTime)
        self.assertEqual(len(ujs), 3)
        for uj in ujs:
            self.assertIn(uj.get('user'), user_list2)
            uj_creation_time = int((uj.get('created') - epoch).total_seconds() * 1000)
            self.assertTrue(minTime <= uj_creation_time <= maxTime)

        # testing list_ujs_results return data, without userIds
        ujs = self.dbi.list_ujs_results([], minTime, maxTime)
        self.assertEqual(len(ujs), 15)

        # testing list_ujs_results return data, with different userIds and times
        ujs = self.dbi.list_ujs_results(['wjriehl'], 1500052541065, 1500074641912)
        self.assertEqual(len(ujs), 8)
        ujs = self.dbi.list_ujs_results([], 1500052541065, 1500074641912)
        self.assertEqual(len(ujs), 14)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_config_str_to_list")
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
    # @unittest.skip("skipped test_MetricsMongoDBController_process_parameters")
    def test_MetricsMongoDBController_process_parameters(self):

        # testing 'user_ids'
        user_list = ['user_1', 'user_2']
        params = {'user_ids': user_list}
        ret_params = self.db_controller.process_parameters(params)
        self.assertItemsEqual(ret_params.get('user_ids'), user_list)

        # no given 'user_ids'
        params = {}
        ret_params = self.db_controller.process_parameters(params)
        self.assertFalse(ret_params['user_ids'])

        # 'user_ids' is not a list
        params = {'user_ids': 'not_a_list_object'}
        with self.assertRaisesRegexp(ValueError,
                                     'Variable user_ids must be a list.'):
            self.db_controller.process_parameters(params)

        # testing removing 'kbasetest'
        user_list_kbasetest = ['user_1', 'user_2', 'kbasetest']
        params = {'user_ids': user_list_kbasetest}
        ret_params = self.db_controller.process_parameters(params)
        self.assertItemsEqual(ret_params.get('user_ids'), user_list)

        # testing epoch_range size 3
        params = {'epoch_range': (1, 2, 3)}
        with self.assertRaisesRegexp(ValueError,
                                     'Invalide epoch_range. Size must be 2.'):
            self.db_controller.process_parameters(params)

        # testing epoch_range
        params = {'epoch_range': ('2018-02-23T00:00:00+0000', '2018-02-25T00:00:00+0000')}
        ret_params = self.db_controller.process_parameters(params)
        self.assertEqual(ret_params.get('minTime'), 1519344000000)
        self.assertEqual(ret_params.get('maxTime'), 1519516800000)
        self.assertFalse(ret_params['user_ids'])

        date_time = datetime.datetime.strptime('2018-02-23T00:00:00+0000',
                                               '%Y-%m-%dT%H:%M:%S+0000')
        date = datetime.datetime.strptime('2018-02-25T00:00:00+0000',
                                          '%Y-%m-%dT%H:%M:%S+0000').date()
        params = {'epoch_range': (date_time, date)}
        ret_params = self.db_controller.process_parameters(params)
        self.assertEqual(ret_params.get('minTime'), 1519344000000)
        self.assertEqual(ret_params.get('maxTime'), 1519516800000)
        self.assertFalse(ret_params['user_ids'])

        params = {'epoch_range': ('2018-02-23T00:00:00+0000', '')}
        ret_params = self.db_controller.process_parameters(params)
        self.assertEqual(ret_params.get('minTime'), 1519344000000)
        self.assertEqual(ret_params.get('maxTime'), 1519516800000)
        self.assertFalse(ret_params['user_ids'])

        params = {'epoch_range': (None, '2018-02-25T00:00:00+0000')}
        ret_params = self.db_controller.process_parameters(params)
        self.assertEqual(ret_params.get('minTime'), 1519344000000)
        self.assertEqual(ret_params.get('maxTime'), 1519516800000)
        self.assertFalse(ret_params['user_ids'])

        # testing empty epoch_range
        params = {'epoch_range': (None, None)}
        ret_params = self.db_controller.process_parameters(params)
        today = datetime.date.today()
        minTime = ret_params.get('minTime')
        maxTime = ret_params.get('maxTime')
        minTime_from_today = (datetime.date(*time.localtime(minTime/1000)[:3]) - today).days
        maxTime_from_today = (datetime.date(*time.localtime(maxTime/1000)[:3]) - today).days
        self.assertEqual(minTime_from_today, -2)
        self.assertEqual(maxTime_from_today, 0)

        params = {}
        ret_params = self.db_controller.process_parameters(params)
        today = datetime.date.today()
        minTime = ret_params.get('minTime')
        maxTime = ret_params.get('maxTime')
        minTime_from_today = (datetime.date(*time.localtime(minTime/1000)[:3]) - today).days
        maxTime_from_today = (datetime.date(*time.localtime(maxTime/1000)[:3]) - today).days
        self.assertEqual(minTime_from_today, -2)
        self.assertEqual(maxTime_from_today, 0)

    # Uncomment to skip this test
    # @unittest.skip("test_db_controller_constructor")
    def test_db_controller_constructor(self):

        expected_admin_list = ['kkeller', 'scanon', 'psdehal', 'dolson', 'nlharris', 'dylan',
                               'chenry', 'ciservices', 'wjriehl', 'sychan', 'jjeffryes',
                               'thomasoniii', 'eapearson', 'qzhang', 'tgu2']
        self.assertItemsEqual(self.db_controller.adminList, expected_admin_list)

        expected_metrics_admin_list = ['scanon', 'psdehal', 'dolson', 'chenry', 'wjriehl',
                                       'sychan', 'qzhang', 'tgu2', 'eapearson']
        self.assertItemsEqual(self.db_controller.metricsAdmins, expected_metrics_admin_list)

        expected_db_list = ['metrics', 'userjobstate', 'workspace', 'exec_engine', 'auth2']
        self.assertItemsEqual(self.db_controller.mongodb_dbList, expected_db_list)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_db_controller_parse_app_id_method")
    def test_db_controller_parse_app_id_method(self):
        exec_tasks =[{
            "job_input" : {
                "method" : "kb_rnaseq_donwloader.export_rna_seq_expression_as_zip",
            }
        },
        {
            "job_input" : {
                "app_id" : "kb_deseq/run_DESeq2",
                "method" : "kb_deseq.run_deseq2_app",
            }
        },
        {
            "job_input" : {
                "app_id" : "kb_cufflinks.run_Cuffdiff",
                "method" : "kb_cufflinks/run_Cuffdiff",
            }
        },
        {
            "job_input" : {
                "app_id" : "kb_deseq/run_DESeq2",
                "method" : "kb_deseq/run_deseq2_app",
            }
        },
        {
            "job_input" : {
                "app_id" : "kb_deseq.run_DESeq2",
                "method" : "kb_deseq.run_deseq2_app",
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
        self.assertEqual(self.db_controller._parse_method(exec_tasks[0]),
                        'kb_rnaseq_donwloader.export_rna_seq_expression_as_zip')
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

        self.assertItemsEqual(self.db_controller._map_narrative(
            wsids[0], ws_narrs), ('', '0'))
        self.assertItemsEqual(self.db_controller._map_narrative(
            wsids[1], ws_narrs), ('', '0'))
        self.assertItemsEqual(self.db_controller._map_narrative(
            wsids[2], ws_narrs), ('Staging Test', '1'))

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_join_task_ujs")
    def test_MetricsMongoDBController_join_task_ujs(self):
        # testing data sets
        exec_tasks =[{
            "ujs_job_id" : "5968cd75e4b08b65f9ff5d7c",
            "job_input" : {
                "method" : "kb_rnaseq_donwloader.export_rna_seq_expression_as_zip",
            },
        },
        {
            "ujs_job_id" : "596832a4e4b08b65f9ff5d6f",
            "job_input" : {
                "app_id" : "kb_deseq/run_DESeq2",
                "method" : "kb_deseq.run_deseq2_app",
                "params" : [
                    {
                        "workspace_name" : "tgu2:1481170361822"
                    }
                ],
                "wsid" : 15206,
            },
        },
        {
            "ujs_job_id" : "5968e5fde4b08b65f9ff5d7d",
            "job_input" : {
                "app_id" : "kb_cufflinks/run_Cuffdiff",
                "method" : "kb_cufflinks.run_Cuffdiff",
                "params" : [
                    {
                        "workspace_name" : "umaganapathyswork:narrative_1498130853194"
                    }
                ],
                "wsid" : 23165,
            },
        }]
        ujs_jobs =[{
            "_id" : "596832a4e4b08b65f9ff5d6f",
            "user" : "tgu2",
            "authstrat" : "kbaseworkspace",
            "authparam" : "15206",
            "created" : 1500000932849,
            "updated" : 1500001203182,
            "estcompl" : None,
            "status" : "done",
            "desc" : "Execution engine job for kb_deseq.run_deseq2_app",
            "started" : 1500000937695,
            "complete" : True,
            "error" : False
        },
        {
            "_id" : "5968cd75e4b08b65f9ff5d7c",
            "user" : "arfath",
            "authstrat" : "DEFAULT",
            "authparam" : "DEFAULT",
            "created" : 1500040565733,
            "updated" : 1500040661079,
            "estcompl" : None,
            "status" : "done",
            "desc" : "Execution engine job for kb_rnaseq_donwloader.export_rna_seq_expression_as_zip",
            "started" : 1500040575585,
            "complete" : True,
            "error" : False
        },
        {
            "_id" : "5968e5fde4b08b65f9ff5d7d",
            "user" : "umaganapathyswork",
            "authstrat" : "kbaseworkspace",
            "authparam" : "23165",
            "created" : 1500046845485,
            "updated" : 1500047709785,
            "estcompl" : None,
            "status" : "done",
            "desc" : "Execution engine job for kb_cufflinks.run_Cuffdiff",
            "started" : 1500046850810,
            "complete" : True,
            "error" : False
        }]
        # testing the correct data items appear in the joined result
        joined_results = self.db_controller.join_task_ujs(exec_tasks, ujs_jobs)
        self.assertEqual(len(joined_results), 3)
        self.assertEqual(joined_results[0]['wsid'], '15206')
        self.assertEqual(joined_results[0]['app_id'], 'kb_deseq/run_DESeq2')
        self.assertEqual(joined_results[0]['method'], 'kb_deseq.run_deseq2_app')
        self.assertEqual(joined_results[0]['finish_time'], 1500001203182)
        self.assertIn('client_groups', joined_results[0])
        self.assertEqual(joined_results[0]['workspace_name'], 'tgu2:1481170361822')

        self.assertNotIn('wsid', joined_results[1])
        self.assertEqual(joined_results[1]['app_id'],
                'kb_rnaseq_donwloader/export_rna_seq_expression_as_zip')
        self.assertEqual(joined_results[1]['method'],
                'kb_rnaseq_donwloader.export_rna_seq_expression_as_zip')
        self.assertEqual(joined_results[1]['finish_time'], 1500040661079)
        self.assertIn('client_groups', joined_results[1])
        self.assertNotIn('workspace_name', joined_results[1])

        self.assertEqual(joined_results[2]['wsid'], '23165')
        self.assertEqual(joined_results[2]['app_id'], 'kb_cufflinks/run_Cuffdiff')
        self.assertEqual(joined_results[2]['method'], 'kb_cufflinks.run_Cuffdiff')
        self.assertEqual(joined_results[2]['workspace_name'],
                'umaganapathyswork:narrative_1498130853194')
        self.assertEqual(joined_results[2]['finish_time'], 1500047709785)
        self.assertIn('client_groups', joined_results[2])

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_get_client_groups_from_cat_")
    def test_MetricsMongoDBController_get_client_groups_from_cat(self):
        # testing if the data has expected structure
        clnt_ret = self.db_controller.get_client_groups_from_cat(
                self.getContext()['token'])
        self.assertIn('app_id', clnt_ret[0])
        self.assertIn('client_groups', clnt_ret[0])
        target_clnt = 'kb_upload'
        for clnt in clnt_ret:
            if target_clnt in clnt['app_id']:
                self.assertIn(target_clnt, clnt['client_groups'])

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_update_user_info
    def test_MetricsMongoDBController_update_user_info(self):
        user_list = ['sulbha', 'ytm123', 'xiaoli','andrew78', 'qzhang']
        start_datetime = datetime.datetime.strptime('2018-01-01T00:00:00+0000',
                                               '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                          '%Y-%m-%dT%H:%M:%S.%fZ')

        # testing get_user_details return data with given user_ids
        params = {'user_ids': user_list}
        params['epoch_range'] = (start_datetime, end_datetime)
        # testing if the data has expected structure
        upd_ret = self.db_controller.update_user_info(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertTrue(upd_ret >= 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_update_daily_activities
    def test_MetricsMongoDBController_update_daily_activities(self):
        user_list = ['sulbha', 'ytm123', 'xiaoli','andrew78', 'qzhang']
        start_datetime = datetime.datetime.strptime('2018-01-01T00:00:00+0000',
                                               '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                          '%Y-%m-%dT%H:%M:%S.%fZ')

        # testing get_user_details return data with given user_ids
        params = {'user_ids': user_list}
        params['epoch_range'] = (start_datetime, end_datetime)
        # testing if the data has expected structure
        upd_ret = self.db_controller.update_daily_activities(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertTrue(upd_ret >= 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_update_narratives
    def test_MetricsMongoDBController_update_narratives(self):
        user_list = ['sulbha', 'ytm123', 'xiaoli','andrew78', 'qzhang']
        start_datetime = datetime.datetime.strptime('2018-01-01T00:00:00+0000',
                                               '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2018-03-31T00:00:10.000Z',
                                          '%Y-%m-%dT%H:%M:%S.%fZ')

        # testing get_user_details return data with given user_ids
        params = {'user_ids': user_list}
        params['epoch_range'] = (start_datetime, end_datetime)
        # testing if the data has expected structure
        upd_ret = self.db_controller.update_narratives(
                self.getContext()['user_id'],
                params, self.getContext()['token'])
        self.assertTrue(upd_ret >= 0)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_get_user_job_states")
    def test_MetricsMongoDBController_get_user_job_states(self):
        # testing if the data has expected structure and values
        user_list = ['tgu2', 'umaganapathyswork', 'arfath']
        params = {'user_ids': user_list}
        start_datetime = datetime.datetime.strptime('2017-07-14T02:55:32+0000',
                                               '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2017-07-14T16:08:53.956Z',
                                          '%Y-%m-%dT%H:%M:%S.%fZ')
        params['epoch_range'] = (start_datetime, end_datetime)
        input_params = self.db_controller.process_parameters(params)
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
        self.assertEqual(ujs[0]['workspace_name'], 'tgu2:1481170361822')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_MetricsMongoDBController_get_user_details")
    def test_MetricsMongoDBController_get_user_details(self):
        user_list0 = []
        user_list = ['sulbha', 'ytm123', 'xiaoli','andrew78', 'qzhang']
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
    # @unittest.skip("skipped test_MetricsMongoDBController_get_active_users_counts")
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

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
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
        self.assertIn('njs', app_metrics_ret[0]['client_groups'])
        self.assertEqual(app_metrics_ret[0]['narrative_name'], 'Staging Test')
        self.assertEqual(app_metrics_ret[0]['workspace_name'], 'psdehal:narrative_1513709108341')
        #print(app_metrics_ret[0])

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_get_user_details")
    def test_run_MetricsImpl_get_user_details(self):
        # testing get_user_details return data with specified user_ids
        user_list = ['sulbha', 'ytm123', 'xiaoli','andrew78', 'qzhang']
        epoch_range = (datetime.datetime(2018, 1, 1), datetime.datetime(2018, 3, 31))
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
        self.assertEqual(len(users), 33)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_get_user_counts_per_day")
    def test_run_MetricsImpl_get_user_counts_per_day(self):
        m_params = {
            'epoch_range': (datetime.datetime(2018, 1, 1), datetime.datetime(2018, 3, 31))
        }
        ret = self.getImpl().get_user_counts_per_day(self.getContext(), m_params)
        # testing (excluding kbstaff by default)
        users = ret[0]['metrics_result']
        self.assertEqual(len(users), 56)
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
            'epoch_range': (datetime.datetime(2018, 2, 28), datetime.datetime(2018, 3, 31))
        }
        ret = self.getImpl().update_metrics(self.getContext(), m_params)
        upds = ret[0]['metrics_result']
        self.assertIn('user_updates', upds)
        self.assertIn('activity_updates', upds)
        self.assertIn('narrative_updates', upds)
        self.assertTrue(upds['user_updates'] >= 0)
        self.assertTrue(upds['activity_updates'] >= 0)
        self.assertTrue(upds['narrative_updates'] >= 0)
