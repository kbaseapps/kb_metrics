# -*- coding: utf-8 -*-
import copy
import datetime
import json  # noqa: F401
import os  # noqa: F401
import time
import unittest
from configparser import ConfigParser
from os import environ
from unittest.mock import patch
import copy
from operator import itemgetter
import io

from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import WriteError, ConfigurationError

from installed_clients.WorkspaceClient import Workspace as workspaceService
from kb_Metrics.Util import _unix_time_millis_from_datetime
from kb_Metrics.authclient import KBaseAuth as _KBaseAuth
from kb_Metrics.kb_MetricsImpl import kb_Metrics
from kb_Metrics.kb_MetricsServer import MethodContext
from kb_Metrics.metrics_dbi import MongoMetricsDBI
from kb_Metrics.metricsdb_controller import MetricsMongoDBController
from kb_Metrics.NarrativeCache import NarrativeCache

debug = False
def print_debug(msg):
    if not debug:
        return
    t = str(datetime.datetime.now())
    print("{}:{}".format(t, msg))


class kb_Metrics_query_jobs_Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print_debug("SETUP CLASS")
        token = environ.get('KB_AUTH_TOKEN', None)
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        print_debug('CFG STARTING')
        for nameval in config.items('kb_Metrics'):
            print_debug('CFG: {} = {}'.format(nameval[0], nameval[1]))
            cls.cfg[nameval[0]] = nameval[1]
        print_debug('CFG FINISHED')
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
        print_debug('SETUP CLASS - about to create a new kb_Metrics')
        cls.serviceImpl = kb_Metrics(cls.cfg)
        cls.scratch = cls.cfg['scratch']
        cls.callback_url = os.environ['SDK_CALLBACK_URL']
        print_debug('SETUP CLASS - about to create a new controller')
        cls.db_controller = MetricsMongoDBController(cls.cfg)
        cls.narrative_cache = NarrativeCache(cls.cfg)
        cls.client = MongoClient(port=27017)
        print_debug("MONGO - about to start")
        cls.init_mongodb()

        test_cfg_file = '/kb/module/work/test.cfg'
        test_cfg_text = "[test]\n"
        with open(test_cfg_file, "r") as f:
            test_cfg_text += f.read()
        config = ConfigParser()
        # config.readfp(io.StringIO.StringIO(test_cfg_text))
        config.read_string(test_cfg_text)
        test_cfg_dict = dict(config.items("test"))
        cls.test_cfg = test_cfg_dict

    @classmethod
    def tearDownClass(cls):
        cls.clear_mongodb()
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    @classmethod
    def clear_mongodb(cls):
        dbs = ['workspace', 'exec_engine', 'userjobstate', 'auth2', 'metrics']
        for db in dbs:
            try:
                cls.client[db].command("dropUser", "admin")
                cls.client.drop_database(db)
            except Exception as ex:
                print('ERROR dropping db: ' + str(ex))

    @classmethod
    def init_mongodb(cls):
        print_debug("MONGO - starting")
        print_debug('starting to build local mongoDB')

        os.system("sudo service mongodb start")
        os.system("mongod --version")
        os.system("cat /var/log/mongodb/mongodb.log "
                  "| grep 'waiting for connections on port 27017'")

        print_debug("MONGO - ready")

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
            updated_str = jrecord.get('updated')
            cls.client.userjobstate.jobstate.update_many(
                {"created": created_str},
                {"$set": {"created": datetime.datetime.utcfromtimestamp(
                                        int(created_str) / 1000.0),
                          "updated": datetime.datetime.utcfromtimestamp(
                                        int(updated_str) / 1000.0)}
                }
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
                                   f'ci_{db_name}.{table}.json')
        json_data = open(record_file).read()
        records = json.loads(json_data)

        if table == 'jobstate':
            for record in records:
                record['_id'] = ObjectId(record['_id'])

        db[table].drop()
        db[table].insert_many(records)
        print_debug(f'Inserted {len(records)} records for {db_name}.{table}')

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
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_no_params")
    def test_run_MetricsImpl_query_jobs_no_params(self):
        ret = self.getImpl().query_jobs(self.getContext(), {
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('total_count', result)
        self.assertEqual(result['total_count'], 38)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_max_time_range")
    def test_run_MetricsImpl_query_jobs_max_time_range(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs(self.getContext(), {
            'epoch_range': [0, now]
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('total_count', result)
        self.assertEqual(result['total_count'], 38)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_max_time_range_paged")
    def test_run_MetricsImpl_query_jobs_max_time_range_paged(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs(self.getContext(), {
            'epoch_range': [0, now],
            'offset': 5,
            'limit': 5
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('total_count', result)
        self.assertEqual(result['total_count'], 38)
        self.assertEqual(len(result['job_states']), 5)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_user")
    def test_run_MetricsImpl_query_jobs_one_user(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs(self.getContext(), {
            'epoch_range': [0, now],
            'user_ids': ['psdehal']
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('total_count', result)
        self.assertEqual(result['total_count'], 1)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_one_job(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs(self.getContext(), {
            'job_ids': ['5a68dffce4b0ace8f870f586']
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('total_count', result)
        self.assertEqual(result['total_count'], 1)