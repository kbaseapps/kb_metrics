# -*- coding: utf-8 -*-
import datetime
import json  # noqa: F401
import os  # noqa: F401
import unittest
from configparser import ConfigParser
from os import environ

from kb_Metrics.kb_MetricsImpl import kb_Metrics
from kb_Metrics.kb_MetricsServer import MethodContext

debug = False


def print_debug(msg):
    if not debug:
        return
    t = str(datetime.datetime.now())
    print("{}:{}".format(t, msg))


class kb_Metrics_status_Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load configuration
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('kb_Metrics'):
            cls.cfg[nameval[0]] = nameval[1]

        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)

        cls.serviceImpl = kb_Metrics(cls.cfg)
        cls.scratch = cls.cfg['scratch']

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    def validate_result(self, result):
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], dict)
        return result[0]

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_status_happy")
    def test_run_MetricsImpl_status_happy(self):
        ret = self.getImpl().status(self.getContext())

        result = self.validate_result(ret)

        self.assertIsInstance(result, dict)
        for key in ['state', 'message', 'version', 'git_url',
                    'git_commit_hash']:
            self.assertIn(key, result)
