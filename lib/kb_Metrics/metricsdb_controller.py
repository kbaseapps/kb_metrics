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

from Workspace.WorkspaceClient import Workspace as Workspace
from Catalog.CatalogClient import Catalog
from NarrativeJobService.NarrativeJobServiceClient import NarrativeJobService
from UserAndJobState.UserAndJobStateClient import UserAndJobState
from UserProfile.UserProfileClient import UserProfile

class MetricsMongoDBController:

    def __init__(self, config):
        pprint("initializing mdb......")
        #pprint(config)
        # first grab the admin list
        self.adminList = []
        if 'admin-users' in config:
            adm_ids = config['admin-users'].split(',')
            for a_id in adm_ids:
                if a_id.strip():
                    self.adminList.append(a_id.strip())
        if not self.adminList:  # pragma: no cover
            warnings.warn('no "admin-users" are set in config of MetricsMongoDBController.')

        # make sure the minimal mongo settings are in place
        if 'mongodb-host' not in config: # pragma: no cover
            raise ValueError('"mongodb-host" config variable must be defined to start a MetricsMongoDBController!')
        if 'mongodb-databases' not in config: # pragma: no cover
            raise ValueError('"mongodb-databases" config variable must be defined to start a MetricsMongoDBController!')
	self.mongodb_dbList = []
        if 'mongodb-databases' in config:
            db_ids = config['mongodb-databases'].split(',')
            for d_id in db_ids:
                if d_id.strip():
                    self.mongodb_dbList.append(d_id.strip())
        if not self.mongodb_dbList:  # pragma: no cover
            warnings.warn('no "mongodb-databases" are set in config of MetricsMongoDBController.')
        # give warnings if no mongo user information is set
        if 'mongodb-user' not in config: # pragma: no cover
            warnings.warn('"mongodb-user" is not set in config of MetricsMongoDBController.')
            config['mongodb-user']=''
            config['mongodb-pwd']=''
        if 'mongodb-pwd' not in config: # pragma: no cover
            warnings.warn('"mongodb-pwd" is not set in config of MetricsMongoDBController.')
            config['mongodb-pwd']=''
        # instantiate the mongo client
        self.metrics_dbi = MongoMetricsDBI(
                    config['mongodb-host'],
 		    self.mongodb_dbList,
                    config['mongodb-user'],
                    config['mongodb-pwd'])

	#initialize clients for accessing other services
	'''
        self.workspace_url = config['workspace-url']
        self.ws_client = Workspace(self.workspace_url, token=token)
        self.cat_client = Catalog('https://ci.kbase.us/services/catalog', auth_svc='https://ci.kbase.us/services/auth/', token=token)
        self.njs_client = NarrativeJobService('https://ci.kbase.us/services/njs_wrapper', auth_svc='https://ci.kbase.us/services/auth/', token=token)
        self.ujs_client = UserAndJobState('https://ci.kbase.us/services/userandjobstate', auth_svc='https://ci.kbase.us/services/auth/', token=token)
        self.uprf_client = UserProfile('https://ci.kbase.us/services/user_profile/rpc', auth_svc='https://ci.kbase.us/services/auth/', token=token)
	'''

    # functions to get the requested records...
    def get_user_job_states(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
        if 'before' in params:
            maxTime = params['before']
        if 'user_ids' in params:
            user_ids = params['user_ids']

        db_ret = self.metrics_dbi.list_user_job_states(user_ids, minTime, maxTime)
        return {'user_job_states': db_ret}


    def get_user_tasks(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
        if 'before' in params:
            maxTime = params['before']
        if 'user_ids' in params:
            user_ids = params['user_ids']

        db_ret = self.metrics_dbi.list_user_tasks(user_ids, minTime, maxTime)
        return {'user_tasks': db_ret}


    def get_user_details(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
        if 'before' in params:
            maxTime = params['before']
        if 'user_ids' in params:
            user_ids = params['user_ids']

        db_ret = self.metrics_dbi.list_user_details(user_ids, minTime, maxTime)
        return {'user_details': db_ret}


    def get_user_jobs(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        minTime = None
        maxTime = None
        user_ids = None
        if 'after' in params:
            minTime = params['after']
        if 'before' in params:
            maxTime = params['before']
        if 'user_ids' in params:
            user_ids = params['user_ids']

        db_ret = self.metrics_dbi.list_user_jobs(user_ids, minTime, maxTime)
        return {'user_jobs': db_ret}


    def is_admin(self, username):
        if username in self.adminList:
            return True
        return False

