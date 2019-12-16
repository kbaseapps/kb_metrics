# -*- coding: utf-8 -*-
import json  # noqa: F401
import os  # noqa: F401
import time
from kb_Metrics.Test import Test

debug = False

TOTAL_COUNT = 48
TIMEOUT = 10000


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
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_max_time_range_paged") # noqa E501
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
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_queue") # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_status_queue(self):
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
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_run") # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_status_run(self):
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
        self.assertEqual(result['found_count'], 29)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_one_job")
    def test_run_MetricsImpl_query_jobs_filter_by_status_error(self):
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

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_status_queue_or_run") # noqa E501

    def test_run_MetricsImpl_query_jobs_filter_by_status_queue_or_run(self):
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
            ret = self.getImpl().query_jobs_admin(
                self.getContext(), datum['input'])
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
                    'found_count': 11
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
                    'found_count': 11
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
            ret = self.getImpl().query_jobs_admin(
                self.getContext(), datum['input'])
            self.assertEqual(len(ret), 1)
            self.assertIsInstance(ret[0], dict)
            result = ret[0]
            self.assertIsInstance(result, dict)
            self.assertIn('job_states', result)
            self.assertIn('found_count', result)
            for k, v in datum['expected'].items():
                self.assertEqual(result[k], v)

    # Uncomment to skip this test
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_job_id_run") # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_job_id_run(self):
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
    # @unittest.skip("skipped test_run_MetricsImpl_query_jobs_filter_by_job_id_2") # noqa E501
    def test_run_MetricsImpl_query_jobs_filter_by_job_id_2(self):
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
