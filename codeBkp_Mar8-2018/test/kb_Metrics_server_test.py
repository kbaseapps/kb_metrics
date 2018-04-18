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
        cls._insert_data(client, 'metrics', 'daily_activities')

        cls.db_names = client.database_names()
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

        cls.dbi =  MongoMetricsDBI('', client.database_names(), 'admin', 'password')

    @classmethod
    def _insert_data(cls, client, db_name, table):

        db = client[db_name]

        record_file = os.path.join('db_files', 'ci_{}.{}.json'.format(db_name, table))
        json_data = open(record_file).read()
        records = json.loads(json_data)

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

    def test_dbi_contructor(self):
        # testing if the db is connected and handshakes cab be made
        exec_cur = self.dbi.metricsDBs['exec_engine']['exec_tasks'].find()
        self.assertEqual(len(list(exec_cur)), 83)
        ws_cur = self.dbi.metricsDBs['workspace']['workspaces'].find()
        self.assertEqual(len(list(ws_cur)), 26)
        wsobj_cur = self.dbi.metricsDBs['workspace']['workspaceObjects'].find()
        self.assertEqual(len(list(wsobj_cur)), 826)
        ujs_cur = self.dbi.metricsDBs['userjobstate']['jobstate'].find()
        self.assertEqual(len(list(ujs_cur)), 35)
        act_cur = self.dbi.metricsDBs['metrics']['daily_activities'].find()
        self.assertEqual(len(list(act_cur)), 75)

    def test_MetricsMongoDBs_list_exec_tasks(self):
        minTime = 1500000932952
        maxTime = 1500046845591

        # testing list_exec_tasks return data
        exec_tasks = self.dbi.list_exec_tasks(minTime, maxTime)
        self.assertEqual(len(exec_tasks), 3)

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

    def test_db_controller_contructor(self):

        expected_admin_list = ['kkeller', 'scanon', 'psdehal', 'dolson', 'nlharris', 'dylan',
                               'chenry', 'ciservices', 'wjriehl', 'sychan', 'jjeffryes',
                               'thomasoniii', 'eapearson', 'qzhang', 'tgu2']
        self.assertItemsEqual(self.db_controller.adminList, expected_admin_list)

        expected_metrics_admin_list = ['scanon', 'psdehal', 'dolson', 'chenry', 'wjriehl',
                                       'sychan', 'qzhang', 'tgu2', 'eapearson']
        self.assertItemsEqual(self.db_controller.metricsAdmins, expected_metrics_admin_list)

        expected_db_list = ['metrics', 'userjobstate', 'workspace', 'exec_engine', 'auth2']
        self.assertItemsEqual(self.db_controller.mongodb_dbList, expected_db_list)

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
                        "fold_change_cutoff" : 1.5,
                        "condition_labels" : [
                            "WT",
                            "hy5"
                        ],
                        "expressionset_ref" : "15206/242/1",
                        "diff_expression_obj_name" : "differential_expr_sample",
                        "fold_scale_type" : "log2",
                        "workspace_name" : "tgu2:1481170361822",
                        "alpha_cutoff" : 1,
                        "num_threads" : 4
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
                        "time_series" : 0,
                        "multi_read_correct" : 0,
                        "expressionset_ref" : "23165/2/1",
                        "output_obj_name" : "hisat2_stringtie_cuffdiff_output",
                        "library_norm_method" : "classic-fpkm",
                        "min_alignment_count" : 10,
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
        self.assertEqual(joined_results[1]['app_id'], 'kb_rnaseq_donwloader/export_rna_seq_expression_as_zip')
        self.assertEqual(joined_results[1]['method'], 'kb_rnaseq_donwloader.export_rna_seq_expression_as_zip')
        self.assertEqual(joined_results[1]['finish_time'], 1500040661079)
        self.assertIn('client_groups', joined_results[1])
        self.assertNotIn('workspace_name', joined_results[1])

        self.assertEqual(joined_results[2]['wsid'], '23165')
        self.assertEqual(joined_results[2]['app_id'], 'kb_cufflinks/run_Cuffdiff')
        self.assertEqual(joined_results[2]['method'], 'kb_cufflinks.run_Cuffdiff')
        self.assertEqual(joined_results[2]['workspace_name'], 'umaganapathyswork:narrative_1498130853194')
        self.assertEqual(joined_results[2]['finish_time'], 1500047709785)
        self.assertIn('client_groups', joined_results[2])

    def test_MetricsMongoDBController_get_jobdata_from_ws_exec_ujs(self):
        # testing if the data has expected structure and values
        user_list = ['tgu2', 'umaganapathyswork', 'arfath']
        params = {'user_ids': user_list}
        start_datetime = datetime.datetime.strptime('2017-07-14T02:55:32+0000',
                                               '%Y-%m-%dT%H:%M:%S+0000')
        end_datetime = datetime.datetime.strptime('2017-07-14T10:40:46+0000',
                                          '%Y-%m-%dT%H:%M:%S+0000')
        params['epoch_range'] = (start_datetime, end_datetime)
        input_params = self.db_controller.process_parameters(params)
        self.assertEqual(input_params.get('minTime'), 1500000932000)
        self.assertEqual(input_params.get('maxTime'), 1500046846000)

        ujs_ret = self.db_controller.get_jobdata_from_ws_exec_ujs(input_params, self.getContext()['token'])
        ujs = ujs_ret['job_states']
        self.assertEqual(len(ujs), 3)
        '''
        self.assertEqual(ujs[0]['wsid'], '15206')
        self.assertEqual(ujs[0]['app_id'], 'kb_deseq/run_DESeq2')
        self.assertEqual(ujs[0]['method'], 'kb_deseq.run_deseq2_app')
        self.assertEqual(ujs[0]['finish_time'], 1500001203182)
        self.assertIn('client_groups', ujs[0])
        self.assertEqual(ujs[0]['workspace_name'], 'tgu2:1481170361822')
        '''

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
    @unittest.skip("skipped test_run_get_app_metrics")
    def test_run_get_app_metrics(self):
        m_params = {
            'user_ids': [], # ['rhizorick'],#'user_ids': [],
            #'epoch_range':(1420083768000,1435677602000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2015,6,30)
            'epoch_range': (u'2017-07-14T00:00:00+0000', u'2017-07-17T17:00:00+0000')
        }
        # Second, call your implementation
        ret = self.getImpl().get_app_metrics(self.getContext(), m_params)
        prnt_count = len(ret[0]['job_states'])
        print("Total number of records returned=" + str(len(ret[0]['job_states'])))
        print(pformat(ret[0]['job_states'][:10]))

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
            'user_ids': ['qzhang'],#'user_ids': [],
            #'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20))
            'epoch_range': (datetime.datetime(2018, 2, 27), datetime.datetime(2018, 2, 28))
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
