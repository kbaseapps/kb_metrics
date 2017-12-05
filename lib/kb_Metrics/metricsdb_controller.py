import warnings
import threading
import time
import copy
import os
import random
import re
import uuid
import codecs


from pprint import pprint
from datetime import datetime
from urlparse import urlparse
from kb_Metrics.metricsDBs import MongoMetricsDBI


class MetricsMongoDBController:

    def __init__(self, config):
        pprint("initializing mdb......")
        pprint(config)
        # first grab the admin list
        self.adminList = []
        if 'admin-users' in config:
            tokens = config['admin-users'].split(',')
            for t in tokens:
                if t.strip():
                    self.adminList.append(t.strip())
        if not self.adminList:  # pragma: no cover
            warnings.warn('no "admin-users" are set in config of MetricsMongoDBController.')

        # make sure the minimal mongo settings are in place
        if 'mongodb-host' not in config: # pragma: no cover
            raise ValueError('"mongodb-host" config variable must be defined to start a MetricsMongoDBController!')
        if 'mongodb-database' not in config: # pragma: no cover
            raise ValueError('"mongodb-database" config variable must be defined to start a MetricsMongoDBController!')

        # give warnings if no mongo user information is set
        if 'mongodb-user' not in config: # pragma: no cover
            warnings.warn('"mongodb-user" is not set in config of MetricsMongoDBController.')
            config['mongodb-user']=''
            config['mongodb-pwd']=''
        if 'mongodb-pwd' not in config: # pragma: no cover
            warnings.warn('"mongodb-pwd" is not set in config of MetricsMongoDBController.')
            config['mongodb-pwd']=''
        # instantiate the mongo client
        db_names = {'userjobstate'}
        self.db = MongoMetricsDBI(
                    config['mongodb-host'],
                    db_names,#config['mongodb-database'],
                    config['mongodb-user'],
                    config['mongodb-pwd'])


    def get_user_job_states(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        self.ws_client = Workspace(self.workspace_url, token=token)
        self.cat_client = Catalog('https://ci.kbase.us/services/catalog', auth_svc='https://ci.kbase.us/services/auth/', token=token)
        self.njs_client = NarrativeJobService('https://ci.kbase.us/services/njs_wrapper', auth_svc='https://ci.kbase.us/services/auth/', token=token)
        self.ujs_client = UserAndJobState('https://ci.kbase.us/services/userandjobstate', auth_svc='https://ci.kbase.us/services/auth/', token=token)
        self.uprf_client = UserProfile('https://ci.kbase.us/services/user_profile/rpc', auth_svc='https://ci.kbase.us/services/auth/', token=token)

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
        if 'before' in params:
            maxTime = params['before']
        if 'user_ids' in params:
            user_ids = params['user_ids']

        return self.db.get_user_job_states(user_ids, minTime, maxTime)


