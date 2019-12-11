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

DEBUG = False

def print_debug(msg):
    if not DEBUG:
        return
    t = str(datetime.datetime.now())
    print ("{}:{}".format(t, msg))

class Test(unittest.TestCase):
    # def __init__(self):
    #     self.debug = false

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
        cls.db_names = cls.client.database_names()

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
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

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
