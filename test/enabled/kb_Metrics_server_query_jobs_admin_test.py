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
from kb_Metrics.Test import Test

debug = False

TOTAL_COUNT = 46
TIMEOUT = 10000

def print_debug(msg):
    if not debug:
        return
    t = str(datetime.datetime.now())
    print ("{}:{}".format(t, msg))

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


class kb_Metrics_query_jobs_admin_Test(Test):
    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_no_params")
    def test_run_MetricsImpl_query_jobs_no_params(self):
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_max_time_range")
    def test_run_MetricsImpl_query_jobs_max_time_range(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'epoch_range': [0, now]
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_max_time_range_paged")
    def test_run_MetricsImpl_query_jobs_max_time_range_paged(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'epoch_range': [0, now],
            'offset': 5,
            'limit': 5
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], TOTAL_COUNT)
        self.assertEqual(len(result['job_states']), 5)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_user")
    def test_run_MetricsImpl_query_jobs_one_user(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'epoch_range': [0, now],
            'filter': {
                'user_id': ['psdehal']
            }
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 1)


    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_one_job(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'filter': {
                'job_id': ['5a68dffce4b0ace8f870f586']
            }
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 1)
        job = result['job_states'][0]
        self.assertEqual(job['app_tag'], 'release')

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_filter_by_status_queue(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'filter': {
                'status': ['queue']
            }
        })
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 2)
        # print('QUEUED', result)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_filter_by_status_run(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'filter': {
                'status': ['run']
            }
        })
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 2)
        # print('RUN', result)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_filter_by_status_complete(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'offset': 0,
            'limit': 10,
            'timeout': TIMEOUT,
            'filter': {
                'status': ['complete']
            }
        })
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        # print('JOBS with complete filter', result)
        self.assertEqual(result['found_count'], 27)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_filter_by_status_error(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'filter': {
                'status': ['error']
            }
        })
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 13)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_filter_by_status_cancel(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'filter': {
                'status': ['terminate']
            }
        })
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 2)


    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_filter_by_status_cancel_or_error(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'filter': {
                'status': ['terminate', 'error']
            }
        })
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 15)


    def test_run_MetricsImpl_query_jobs_filter_by_status_queue_or_run(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'filter': {
                'status': ['queue', 'run']
            }
        })
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 4)

    def test_run_MetricsImpl_query_jobs_sort2(self):
        total_count = TOTAL_COUNT
        test_data = [
            ['updated', 'finish_time', 1414192660701, 1516822657338],
            ['created', 'finish_time', 1414192660701, 1516822657338],
            ['user_id', 'user', 'arfath', 'wjriehl']
        ]
        data = []
        for field, expected_field, first, last in test_data:
            for direction in ['ascending', 'descending']:
                if direction == 'descending':
                    temp = first
                    first = last
                    last = temp
                data.append({
                    'input': {
                        'sort': [
                            {
                                'field': field,
                                'direction': direction
                            }
                        ]
                    },
                    'expected': [
                        {
                            'item': 0,
                            'fields': dict([(expected_field, first)])
                        },
                        {
                            'item': 'last',
                            'fields': dict([(expected_field, last)])
                        }
                    ]
                })

        for datum in data:
            ret = self.getImpl().query_jobs_admin(self.getContext(), datum['input'])
            self.assertEqual(len(ret), 1)
            self.assertIsInstance(ret[0], dict)
            result = ret[0]
            self.assertIsInstance(result, dict) 
            self.assertIn('job_states', result)
            self.assertIn('found_count', result)
            self.assertIn('total_count', result)
            self.assertEqual(result['found_count'], total_count)
            self.assertEqual(result['total_count'], total_count)
            jobs = result['job_states']
            for expected in datum['expected']:
                item = expected['item']
                if type(item) is str:
                    if item == 'last':
                        item = len(jobs) - 1
                job = jobs[item]
                for k, v in expected['fields'].items():
                    self.assertEqual(job[k], v)

    def test_run_MetricsImpl_query_jobs_search(self):
        data = [
            # this one catches eapearson username
            {
                'input': {
                    'search': [
                        {
                            'term': '^eap',
                            'type': 'regex'
                        }
                    ]
                },
                'expected': {
                    'found_count': 9
                }
            },
            {
                'input': {
                    'search': [
                        {
                            'term': 'eapearson',
                            'type': 'exact'
                        }
                    ]
                },
                'expected': {
                    'found_count': 9
                }
            },
            # this one should catch one job by id
            {
                'input': {
                    'search': [
                        {
                            'term': '5d4493a9aa5a4d298c5dc930',
                            'type': 'exact'
                        }
                    ]
                },
                'expected': {
                    'found_count': 1
                }
            },
            # this one should catch one job by id
            {
                'input': {
                    'search': [
                        {
                            'term': '5d4493a9aa5a4d298c5dc930',
                            'type': 'regex'
                        }
                    ]
                },
                'expected': {
                    'found_count': 1
                }
            },
            # this one should catch one job by id and eapearson
            {
                'input': {
                    'search': [
                        {
                            'term': '5d4493a9aa5a4d298c5dc930',
                            'type': 'exact'
                        },
                        {
                            'term': '^eap',
                            'type': 'regex'
                        }
                    ]
                },
                'expected': {
                    'found_count': 1
                }
            },
            # this one should catch one job by id and user zzz, who
            # doesn't exist in the test data, so nothing should be found.
            {
                'input': {
                    'search': [
                        {
                            'term': '5d4493a9aa5a4d298c5dc930',
                            'type': 'exact'
                        },
                        {
                            'term': '^zzz',
                            'type': 'regex'
                        }
                    ]
                },
                'expected': {
                    'found_count': 0,
                    'total_count': TOTAL_COUNT
                }
            }
        ]

        for datum in data:
            ret = self.getImpl().query_jobs_admin(self.getContext(), datum['input'])
            self.assertEqual(len(ret), 1)
            self.assertIsInstance(ret[0], dict)
            result = ret[0]
            self.assertIsInstance(result, dict) 
            self.assertIn('job_states', result)
            self.assertIn('found_count', result)
            for k,v in datum['expected'].items():
                self.assertEqual(result[k], v)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_filter_by_job_id_run(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'filter': {
                'job_id': ['596832a4e4b08b65f9ff5d6f']
            }
        })
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 1)
        

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_filter_by_job_id_run(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs_admin(self.getContext(), {
            'filter': {
                'job_id': ['5d4493a9aa5a4d298c5dc930']
            }
        })
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict) 
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 1)
