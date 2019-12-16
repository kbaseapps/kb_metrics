# -*- coding: utf-8 -*-
import json  # noqa: F401
import os  # noqa: F401
import time
from kb_Metrics.Test import Test

# TODO: improve the test data -- the cases for this are for the test user,
# and the test user "eapearson" (how I am testing at the moment) does not
# have exec state.

TOTAL_COUNT = 11


class kb_Metrics_query_jobs_Test(Test):

    # def mock_MongoMetricsDBI(self, mongo_host, mongo_dbs,
    #                          mongo_user, mongo_psswd):
    #     self.mongo_clients = dict()
    #     self.metricsDBs = dict()
    #     for m_db in mongo_dbs:
    #         self.mongo_clients[m_db] = MongoClient()
    #         self.metricsDBs[m_db] = self.mongo_clients[m_db][m_db]

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
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], TOTAL_COUNT)
        self.assertEqual(result['total_count'], TOTAL_COUNT)

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
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], TOTAL_COUNT)
        self.assertEqual(result['total_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_max_time_range_paged") # noqa E501
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
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], TOTAL_COUNT)
        self.assertEqual(result['total_count'], TOTAL_COUNT)
        self.assertEqual(len(result['job_states']), 5)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_user")
    def test_run_MetricsImpl_query_jobs_one_user(self):
        now = int(round(time.time() * 1000))
        ret = self.getImpl().query_jobs(self.getContext(), {
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
        self.assertEqual(result['found_count'], 0)
        self.assertEqual(result['total_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_one_job(self):
        ret = self.getImpl().query_jobs(self.getContext(), {
            'filter': {
                'job_id': ['59691279e4b08b65f9ff5d87']
            }
        })

        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict)
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 1)
        self.assertEqual(result['total_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_queue") # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_status_queue(self):
        ret = self.getImpl().query_jobs(self.getContext(), {
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
        self.assertEqual(result['found_count'], 1)
        self.assertEqual(result['total_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_run") # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_status_run(self):
        ret = self.getImpl().query_jobs(self.getContext(), {
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
        self.assertEqual(result['total_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_complete") # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_status_complete(self):
        ret = self.getImpl().query_jobs(self.getContext(), {
            'filter': {
                'status': ['complete']
            }
        })
        # print('filter by status complete', ret)
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict)
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 2)
        self.assertEqual(result['total_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_error")  # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_status_error(self):
        ret = self.getImpl().query_jobs(self.getContext(), {
            'filter': {
                'status': ['error']
            }
        })
        # print('filter by status error', json.dumps(ret))
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict)
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 5)
        self.assertEqual(result['total_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_cancel") # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_status_cancel(self):
        ret = self.getImpl().query_jobs(self.getContext(), {
            'filter': {
                'status': ['cancel']
            }
        })
        # print('filter by status error', json.dumps(ret))
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict)
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 11)
        self.assertEqual(result['total_count'], 11)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_cancel_or_error") # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_status_cancel_or_error(self):
        ret = self.getImpl().query_jobs(self.getContext(), {
            'filter': {
                'status': ['cancel', 'error']
            }
        })
        # print('filter by status error', json.dumps(ret))
        self.assertEqual(len(ret), 1)
        self.assertIsInstance(ret[0], dict)
        result = ret[0]
        self.assertIsInstance(result, dict)
        self.assertIn('job_states', result)
        self.assertIn('found_count', result)
        self.assertEqual(result['found_count'], 5)
        self.assertEqual(result['total_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_queue_or_run") # noqa E501

    def test_run_MetricsImpl_query_jobs_filter_by_status_queue_or_run(self):
        ret = self.getImpl().query_jobs(self.getContext(), {
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
        self.assertEqual(result['found_count'], 3)
        self.assertEqual(result['total_count'], TOTAL_COUNT)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_sort2")
    def test_run_MetricsImpl_query_jobs_sort2(self):
        test_data = [
            # Can't test for updated -- it is not always populated!
            # ['updated', 'modification_time', None, 1516822657338],
            ['created', 'creation_time', 1500004550060, 1516822524141],
        ]
        cases = []
        for field, expected_field, first, last in test_data:
            for direction in ['ascending', 'descending']:
                if direction == 'descending':
                    temp = first
                    first = last
                    last = temp
                cases.append({
                    'input': {
                        'sort': [
                            {
                                'field': field,
                                'direction': direction
                            }
                        ],
                        'offset': 0,
                        'limit': 100
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

        for case in cases:
            ret = self.getImpl().query_jobs(self.getContext(), case['input'])
            self.assertEqual(len(ret), 1)
            self.assertIsInstance(ret[0], dict)
            result = ret[0]
            self.assertIsInstance(result, dict)
            self.assertIn('job_states', result)
            jobs = result['job_states']
            # print("\n\nJOBS", jobs)

            self.assertIn('found_count', result)
            self.assertIn('total_count', result)
            self.assertEqual(result['found_count'], TOTAL_COUNT)
            self.assertEqual(result['total_count'], TOTAL_COUNT)

            for expected in case['expected']:
                item = expected['item']
                if type(item) is str:
                    if item == 'last':
                        item = len(jobs) - 1
                job = jobs[item]
                for k, v in expected['fields'].items():
                    self.assertEqual(job.get(k), v)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_search")
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
                    'found_count': TOTAL_COUNT
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
                    'found_count': TOTAL_COUNT
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
            ret = self.getImpl().query_jobs(self.getContext(), datum['input'])
            self.assertEqual(len(ret), 1)
            self.assertIsInstance(ret[0], dict)
            result = ret[0]
            self.assertIsInstance(result, dict)
            self.assertIn('job_states', result)
            self.assertIn('found_count', result)
            for k, v in datum['expected'].items():
                self.assertEqual(result[k], v)
