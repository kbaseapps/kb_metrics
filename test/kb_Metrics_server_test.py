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

        client = MongoClient(port=27017)

        cls._insert_data(client, 'workspace', 'workspaces')
        cls._insert_data(client, 'exec_engine', 'exec_tasks')
        # cls._insert_data(client, 'exec_engine', 'exec_apps')
        cls._insert_data(client, 'userjobstate', 'jobstate')

        # updating created to timstamp field for userjobstate.jobstate
        for record in client.userjobstate.jobstate.find():
            created_str = record.get('created')
            client.userjobstate.jobstate.update_many(
                {"created": created_str},
                {"$set": {"created": datetime.datetime.utcfromtimestamp(int(created_str) / 1000)}}
            )

        for db in client.database_names():
            if db != 'local':
                client[db].command("createUser", "admin", pwd="password", roles=["readWrite"])

    @classmethod
    def _insert_data(cls, client, db_name, table):

        db = client[db_name]

        record_file = os.path.join('db_files', '{}.{}.JSON'.format(db_name, table))
        json_data = open(record_file).read()
        records = json.loads(json_data)

        db[table].insert_many(records)
        print ('Inserted records for {}.{}'.format(db_name, table))

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

    def test_db_controller_contrusctor(self):

        expected_admin_list = ['kkeller', 'scanon', 'psdehal', 'dolson', 'nlharris', 'dylan',
                               'chenry', 'ciservices', 'wjriehl', 'sychan', 'jjeffryes',
                               'thomasoniii', 'eapearson', 'qzhang', 'tgu2']
        self.assertItemsEqual(self.db_controller.adminList, expected_admin_list)

        expected_metrics_admin_list = ['scanon', 'psdehal', 'dolson', 'chenry', 'wjriehl',
                                       'sychan', 'qzhang', 'tgu2']
        self.assertItemsEqual(self.db_controller.metricsAdmins, expected_metrics_admin_list)

        expected_db_list = ['metrics', 'userjobstate', 'workspace', 'exec_engine', 'auth2']
        self.assertItemsEqual(self.db_controller.mongodb_dbList, expected_db_list)

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_total_logins")
    def test_run_get_total_logins(self):
        m_params = {
            # (datetime.datetime(2017, 9, 30), datetime.datetime(2017,12,31)
            'epoch_range': (1506815999000, 1514764799000)
            #'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
        }
        # Second, call your implementation
        ret = self.getImpl().get_total_logins(self.getContext(), m_params)
        print("get_total_logins returns {} records:\n".format(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result']))

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_logins")
    def test_run_get_user_logins(self):
        m_params = {
            # (datetime.datetime(2017, 9, 30), datetime.datetime(2017,12,31)
            'epoch_range': (1506815999000, 1514764799000)
            #'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_logins(self.getContext(), m_params)
        print("get_user_logins returns {} records:\n".format(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result']))

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_ws")
    def test_run_get_user_ws(self):
        m_params = {
            # (datetime.datetime(2017, 9, 30), datetime.datetime(2017,12,31)
            'epoch_range': (1506815999000, 1514764799000)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_ws(self.getContext(), m_params)
        print("get_user_ws returns {} records:\n".format(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result']))

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_narratives")
    def test_run_get_user_narratives(self):
        m_params = {
            # (datetime.datetime(2017, 9, 30), datetime.datetime(2017,12,31)
            'epoch_range': (1506815999000, 1514764799000)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_narratives(self.getContext(), m_params)
        print("get_user_narratives returns {} records:\n".format(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result']))

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_numObjs")
    def test_run_get_user_numObjs(self):
        m_params = {
            # (datetime.datetime(2017, 9, 30), datetime.datetime(2017,12,31)
            'epoch_range': (1506815999000, 1514764799000)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_numObjs(self.getContext(), m_params)
        print("get_user_numObjs returns {} records:\n".format(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result']))

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_get_app_metrics")
    def test_run_get_app_metrics(self):
        m_params = {
            'user_ids': [],  # ['rhizorick'],#'user_ids': [],
            #'epoch_range':(1420083768000,1435677602000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2015,6,30)
            #'epoch_range':(1420083768000,1451606549000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2016,1,1)
            #'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
            'epoch_range': (u'2018-02-23T00:00:00+0000', u'2018-02-28T17:29:42+0000')
        }
        # Second, call your implementation
        ret = self.getImpl().get_app_metrics(self.getContext(), m_params)
        prnt_count = len(ret[0]['job_states']) - 20
        print("Total number of records returned=" + str(len(ret[0]['job_states'])))
        print(pformat(ret[0]['job_states'][prnt_count:len(ret[0]['job_states']) - 10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_exec_apps")
    def test_run_get_exec_apps(self):
        m_params = {
            'user_ids': [],  # ['qzhang'],#'user_ids': [],
            # (datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
            'epoch_range': (1420083768000, 1505876263000)
        }
        # Second, call your implementation
        ret = self.getImpl().get_exec_apps(self.getContext(), m_params)
        print(pformat(ret[0]['metrics_result'][0:10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_exec_tasks")
    def test_run_get_exec_tasks(self):
        m_params = {
            'user_ids': [],  # ['qzhang'],#'user_ids': [],
            # (datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
            'epoch_range': (1420083768000, 1505876263000)
        }
        # Second, call your implementation
        ret = self.getImpl().get_exec_tasks(self.getContext(), m_params)
        print(pformat(ret[0]['metrics_result'][0:10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_ujs_results")
    def test_run_get_user_ujs_results(self):
        m_params = {
            'user_ids': ['qzhang'],  # 'user_ids': [],
            #'epoch_range':(1420083768000,1435677602000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2015,6,30)
            #'epoch_range':(1420083768000,1451606549000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2016,1,1)
            # (datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
            'epoch_range': (1420083768000, 1505876263000)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_ujs_results(self.getContext(), m_params)
        print("Number of records get_user_ujs_results returned=" +
              str(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result'][0:10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_job_states")
    def test_run_get_user_job_states(self):
        m_params = {
            'user_ids': [],  # ['qzhang'],#'user_ids': [],
            # (datetime.datetime(2015, 6, 1), datetime.datetime(2015,12,31)
            'epoch_range': (1435677602000, 1451575202000)
            #'epoch_range':(1420083768000,1451606549000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2016,1,1)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_ujs_results(self.getContext(), m_params)
        print("Total number of records get_user_ujs_results returned=" +
              str(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result'][0:10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_update_metrics")
    def test_run_update_metrics(self):
        m_params = {
            'user_ids': [],  # ['qzhang'],#'user_ids': [],
            #'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20))
            'epoch_range': (datetime.datetime(2018, 2, 24), datetime.datetime(2018, 2, 28))
        }
        # Second, call your implementation
        ret = self.getImpl().update_metrics(self.getContext(), m_params)
        if not ret[0]['metrics_result'] is None:
            print(ret[0]['metrics_result'])

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_details")
    def test_run_get_user_details(self):
        m_params = {
            'user_ids': [],  # ['qzhang'],#'user_ids': [],
            #'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20))
            'epoch_range': (datetime.datetime(2016, 1, 1), datetime.datetime(2018, 6, 28))
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_details(self.getContext(), m_params)
        print("Total number of records get_user_details returned=" +
              str(len(ret[0]['metrics_result'])))
        # print(pformat(ret[0]['metrics_result']))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_activities")
    def test_run_get_user_activities(self):
        m_params = {
            'user_ids': [],  # ['qzhang'],#'user_ids': [],
            #'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20))
            'epoch_range': (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 6, 28))
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_activities(self.getContext(), m_params)
        print("Total number of records get_user_activities returned=" +
              str(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result']))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_counts_per_day")
    def test_run_get_user_counts_per_day(self):
        m_params = {
            'user_ids': [],  # ['qzhang'],#'user_ids': [],
            #'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20))
            'epoch_range': (datetime.datetime(2017, 1, 1), datetime.datetime(2017, 6, 28))
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_counts_per_day(self.getContext(), m_params)
        print("Total number of records get_user_counts_per_day returned=" +
              str(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result']))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_narratives")
    def test_run_get_user_narratives(self):
        m_params = {
            'user_ids': ['psdehal'],  # ['qzhang'],#'user_ids': [],
            #'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20))
            'epoch_range': (datetime.datetime(2014, 1, 1), datetime.datetime(2018, 2, 28))
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_narratives(self.getContext(), m_params)
        #print("Total number of records get_user_narratives returned="+str(len(ret[0]['metrics_result'])))
        print(pformat(ret[0]['metrics_result']))
