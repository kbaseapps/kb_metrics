# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
import requests

from os import environ
try:
    from ConfigParser import ConfigParser  # py2
except:
    from configparser import ConfigParser  # py3

from pprint import pprint, pformat  # noqa: F401

#from biokbase.catalog.Client import Catalog
from biokbase.workspace.client import Workspace as workspaceService
from kb_Metrics.kb_MetricsImpl import kb_Metrics
from kb_Metrics.kb_MetricsServer import MethodContext
from kb_Metrics.authclient import KBaseAuth as _KBaseAuth

from AssemblyUtil.AssemblyUtilClient import AssemblyUtil

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

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    #@unittest.skip("skipped test_run_get_app_metrics")
    def test_run_get_app_metrics(self):
        m_params = {
            'user_ids':[],#['qzhang'],#'user_ids': [],
            'epoch_range':(1420083768000,1435677602000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2015,6,30)
            #'epoch_range':(1420083768000,1451606549000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2016,1,1)
        }
        # Second, call your implementation
        ret = self.getImpl().get_app_metrics(self.getContext(), m_params)
        prnt_count = len(ret[0]['job_states']) - 10
        print("Total number of records returned="+str(len(ret[0]['job_states'])))
        print(pformat(ret[0]['job_states'][prnt_count:]))

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_metrics")
    def test_run_get_user_metrics(self):
        m_params = {
            'user_ids':[],#['qzhang'],#'user_ids': [],
            'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_metrics(self.getContext(), m_params)
        print("get_user_metrics returns {} records:\n".format(len(ret[0]['user_details'])))
	print(pformat(ret[0]['user_details'][0:10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_job_states")
    def test_run_get_user_job_states(self):
        m_params = {
            'user_ids':[],#['qzhang'],#'user_ids': [],
            'epoch_range':(1435677602000,1451575202000)#(datetime.datetime(2015, 6, 1), datetime.datetime(2015,12,31)
            #'epoch_range':(1420083768000,1451606549000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2016,1,1)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_job_states(self.getContext(), m_params)
        print("Total number of records returned="+str(len(ret[0]['ujs_results'])))
        print(pformat(ret[0]['ujs_results'][0:10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_exec_apps")
    def test_run_get_exec_apps(self):
        m_params = {
            'user_ids':[],#['qzhang'],#'user_ids': [],
            'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
        }
        # Second, call your implementation
        ret = self.getImpl().get_exec_apps(self.getContext(), m_params)
        print(pformat(ret[0]['user_apps'][0:10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_exec_tasks")
    def test_run_get_exec_tasks(self):
        m_params = {
            'user_ids':[],#['qzhang'],#'user_ids': [],
            'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
        }
        # Second, call your implementation
        ret = self.getImpl().get_exec_tasks(self.getContext(), m_params)
        print(pformat(ret[0]['user_tasks'][0:10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_ujs_results")
    def test_run_get_user_ujs_results(self):
        m_params = {
            'user_ids':[],#['qzhang'],#'user_ids': [],
            #'epoch_range':(1420083768000,1435677602000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2015,6,30)
            #'epoch_range':(1420083768000,1451606549000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2016,1,1)
            'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_ujs_results(self.getContext(), m_params)
        print("Total number of records returned="+str(len(ret[0]['ujs_results'])))
        print(pformat(ret[0]['ujs_results'][0:10]))

    # Uncomment to skip this test
    @unittest.skip("skipped test_run_get_user_details")
    def test_run_get_user_details(self):
        m_params = {
            'user_ids':[],#['qzhang'],#'user_ids': [],
            'epoch_range':(1420083768000, 1505876263000)#(datetime.datetime(2015, 1, 1), datetime.datetime(2017,9,20)
        }
        # Second, call your implementation
        ret = self.getImpl().get_user_details(self.getContext(), m_params)
        print(pformat(ret[0]['user_details'][0:10]))
