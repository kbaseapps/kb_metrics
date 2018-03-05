import warnings
import time
import datetime
import copy
import re
from bson.objectid import ObjectId

from kb_Metrics.metricsDBs import MongoMetricsDBI
from kb_Metrics.Util import _unix_time_millis_from_datetime, _convert_to_datetime
from Catalog.CatalogClient import Catalog


def log(message, prefix_newline=False):
    """
    Logging function, provides a hook to suppress or redirect log messages.
    """
    print(('\n' if prefix_newline else '') + 
          '{0:.2f}'.format(time.time()) + ': ' + str(message))


class MetricsMongoDBController:

    def _config_str_to_list(self, list_str):

        user_list = list()
        if list_str:
            user_list = [x.strip() for x in list_str.split(',') if x.strip()]
        else:
            warnings.warn('no {} are set in config of MetricsMongoDBController'.format(list_str))

        return user_list

    def __init__(self, config):

        log("initializing mdb......")

        # grab config lists
        self.adminList = self._config_str_to_list(config.get('admin-users'))
        self.metricsAdmins = self._config_str_to_list(config.get('metrics-admins'))
        self.kbstaffList = self._config_str_to_list(config.get('kbase-staff'))
        self.mongodb_dbList = self._config_str_to_list(config.get('mongodb-databases'))

        # check for required parameters
        for p in ['mongodb-host', 'mongodb-databases']:
            if p not in config:
                error_msg = '"{}" config variable must be defined '.format(p)
                error_msg += 'to start a MetricsMongoDBController!'
                raise ValueError(error_msg)

        # give warnings if no mongo user/pw information is set
        for p in ['mongodb-user', 'mongodb-pwd']:
            if p not in config:
                warning_msg = '"{}" is not set in config of MetricsMongoDBController.'.format(p)
                warnings.warn(warning_msg)

        # instantiate the mongo client
        self.metrics_dbi = MongoMetricsDBI(config.get('mongodb-host'), self.mongodb_dbList,
                                           config.get('mongodb-user', ''),
                                           config.get('mongodb-pwd', ''))

        # for access to the Catalog API
        self.auth_service_url = config['auth-service-url']
        self.catalog_url = config['kbase-endpoint'] + '/catalog'

        # commonly used data
        self.ws_narratives = None
        self.client_groups = None

    # function(s) to update the metrics db
    def update_metrics(self, requesting_user, params, token):
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to invoke this action.')

        # 0. get the ws_narrative and client_groups data for lookups
        if self.ws_narratives is None:
            self.ws_narratives = self.metrics_dbi.list_ws_narratives()
        if self.client_groups is None:
            self.client_groups = self.get_client_groups_from_cat(token)

        # 1. update users
        action_result1 = self.update_user_info(requesting_user, params, token)

        # 2. update activities
        action_result2 = self.update_daily_activities(requesting_user, params, token)

        # 3. update narratives
        action_result3 = self.update_narratives(requesting_user, params, token)

        return {'metrics_result': {'user_updates': action_result1,
                                   'activity_updates': action_result2,
                                   'narrative_updates': action_result3}}

    def update_user_info(self, requesting_user, params, token):
        """
        update user info
        If match not found, insert that record as new.
        """
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to invoke this action.')

        params = self.process_parameters(params)
        # TODO: set the param['minTime'] and param['maxTime'] to a given time window,
        # such as most current 24 hours instead of 48 hours as for others.
        auth2_ret = self.metrics_dbi.aggr_user_details(
            params['user_ids'], params['minTime'], params['maxTime'])
        updData = 0
        if len(auth2_ret) == 0:
            print("No user records returned for update!")
            return updData

        print('Retrieved {} user record(s)'.format(len(auth2_ret)))
        idKeys = ['username', 'email']
        dataKeys = ['full_name', 'signup_at', 'last_signin_at', 'roles']
        for u_data in auth2_ret:
            filterByKey = lambda keys: {x: u_data[x] for x in keys}
            idData = filterByKey(idKeys)
            userData = filterByKey(dataKeys)
            isKbstaff = 1 if idData['username'] in self.kbstaffList else 0
            update_ret = self.metrics_dbi.update_user_records(
                idData, userData, isKbstaff)
        updData += update_ret.raw_result['nModified']

        return updData

    def update_daily_activities(self, requesting_user, params, token):
        """
        update user activities reported from Workspace.
        If match not found, insert that record as new.
        """
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to invoke this action.')

        ws_ret = self.get_activities_from_wsobjs(requesting_user, params, token)
        act_list = ws_ret['metrics_result']
        updData = 0
        if len(act_list) == 0:
            print("No activity records returned for update!")
            return updData

        print('Retrieved activities of {} record(s)'.format(len(act_list)))
        idKeys = ['_id']
        countKeys = ['obj_numModified']
        for a_data in act_list:
            filterByKey = lambda keys: {x: a_data[x] for x in keys}
            idData = filterByKey(idKeys)
            countData = filterByKey(countKeys)
            update_ret = self.metrics_dbi.update_activity_records(
                idData, countData)
            updData += update_ret.raw_result['nModified']

        return updData

    def insert_daily_activities(self, requesting_user, params, token):
        """
        insert user activities reported from Workspace.
        If duplicated ids found, skip that record.
        """
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to invoke this action.')

        ws_ret = self.get_activities_from_wsobjs(
            requesting_user, params, token)
        act_list = ws_ret['metrics_result']
        if len(act_list) == 0:
            print("No activity records returned for insertion!")
            return {'metrics_result': []}

        print('Retrieved activities of {} record(s)'.format(len(act_list)))

        for al in act_list:  # set default for inserting records at the first time
            al['recordLastUpdated'] = datetime.datetime.utcnow()

        try:
            insert_ret = self.metrics_dbi.insert_activity_records(act_list)
        except Exception as e:
            print(e)
            return {'metrics_result': e}
        else:
            return {'metrics_result': insert_ret}

    def insert_narratives(self, requesting_user, params, token):
        """
        insert narratives reported from Workspaces and workspaceObjects.
        If duplicated ids found, skip that record.
        """
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to invoke this action.')

        ws_ret = self.get_narratives_from_wsobjs(requesting_user, params, token)
        narr_list = ws_ret['metrics_result']

        if len(narr_list) == 0:
            print("No narrative records returned for insertion!")
            return {'metrics_result': []}

        print('Retrieved narratives of {} record(s)'.format(len(narr_list)))
        for wn in narr_list:  # set default for inserting records at the first time
            wn['recordLastUpdated'] = datetime.datetime.utcnow()
            if wn.get('first_access', None) is None:
                wn[u'first_access'] = wn['last_saved_at']
                wn['access_count'] = 1

        try:
            insert_ret = self.metrics_dbi.insert_narrative_records(narr_list)
        except Exception as e:
            print(e)
            return {'metrics_result': e}
        else:
            return {'metrics_result': insert_ret}

    def update_narratives(self, requesting_user, params, token):
        """
        update user narratives reported from Workspace.
        If match not found, insert that record as new.
        """
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to invoke this action.')

        ws_ret = self.get_narratives_from_wsobjs(requesting_user, params, token)
        narr_list = ws_ret['metrics_result']
        updData = 0
        if len(narr_list) == 0:
            print("No narrative records returned for update!")
            return updData

        print('Retrieved {} narratives record(s)'.format(len(narr_list)))
        idKeys = ['object_id', 'workspace_id']
        otherKeys = ['name', 'last_saved_at', 'last_saved_by', 'numObj',
                     'deleted', 'object_version', 'nice_name', 'latest', 'desc']
        for n_data in narr_list:
            filterByKey = lambda keys: {x: n_data[x] for x in keys}
            idData = filterByKey(idKeys)
            otherData = filterByKey(otherKeys)
            update_ret = self.metrics_dbi.update_narrative_records(idData, otherData)
            updData += update_ret.raw_result['nModified']

        return updData

    # End functions to write to the metrics database

    # functions to get the requested records from metrics db...
    def get_active_users_counts(self, requesting_user, params, token, exclude_kbstaff=True):
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)

        if exclude_kbstaff:
            mt_ret = self.metrics_dbi.aggr_unique_users_per_day(params['minTime'],
                                                                params['maxTime'],
                                                                self.kbstaffList)
        else:
            mt_ret = self.metrics_dbi.aggr_unique_users_per_day(
                params['minTime'], params['maxTime'], [])

        if len(mt_ret) == 0:
            print("No records returned!")

        return {'metrics_result': mt_ret}

    def get_user_details(self, requesting_user, params, token, exclude_kbstaff=False):
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        mt_ret = self.metrics_dbi.get_user_info(params['user_ids'], params['minTime'],
                                                params['maxTime'], exclude_kbstaff)
        if len(mt_ret) == 0:
            print("No records returned!")
        else:
            mt_ret = self.convert_isodate_to_millis(mt_ret,
                                                    ['signup_at', 'last_signin_at'])
        return {'metrics_result': mt_ret}

    def get_activities(self, requesting_user, params, token):
        # TODO not yet pointing to the metrics db yet
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')
        return self.get_activities_from_wsobjs(requesting_user, params, token)

    def get_narratives(self, requesting_user, params, token):
        # TODO not yet pointing to the metrics db yet
        if not self.is_metrics_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')
        return self.get_narratives_from_wsobjs(requesting_user, params, token)

    # End functions to get the requested records from metrics db

    # functions to get the requested records from other dbs...
    def get_narratives_from_wsobjs(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
        params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        # ws_narrs = self.metrics_dbi.list_ws_narratives()
        ws_narrs = copy.deepcopy(self.ws_narratives)
        wsobjs = self.metrics_dbi.list_user_objects_from_wsobjs(
            params['minTime'], params['maxTime'])

        ws_narrs1 = []
        for wn in ws_narrs:
            for obj in wsobjs:
                if wn['workspace_id'] == obj['workspace_id']:
                    if wn['name'] == obj['object_name']:
                        wn[u'object_id'] = obj['object_id']
                        wn[u'object_version'] = obj['object_version']
                        wn[u'latest'] = obj['latest']
                        break
                elif ':' in wn['name']:
                    wts = wn['name'].split(':')[1]
                    p = re.compile(wts, re.IGNORECASE)
                    if p.search(obj['object_name']):
                        wn[u'object_id'] = obj['object_id']
                        wn[u'object_version'] = obj['object_version']
                        wn[u'latest'] = obj['latest']
                        break

        for wn in ws_narrs:
            if not wn.get('object_id', None) is None:
                wn[u'last_saved_by'] = wn['username']
                del wn['username']

                wn[u'nice_name'] = ''
                if not wn.get('meta', None) is None:
                    w_meta = wn['meta']
                    for w_m in w_meta:
                        if w_m['k'] == 'narrative_nice_name':
                            wn[u'nice_name'] = w_m['v']
                    del wn['meta']
                ws_narrs1.append(wn)
        return {'metrics_result': ws_narrs1}

    def get_activities_from_wsobjs(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
        params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        wsobjs_act = self.metrics_dbi.aggr_activities_from_wsobjs(
            params['minTime'], params['maxTime'])
        ws_owners = self.metrics_dbi.list_ws_owners()

        for wo in ws_owners:
            for obj in wsobjs_act:
                if wo['ws_id'] == obj['_id']['ws_id']:
                    obj['_id'][u'username'] = wo['username']
        return {'metrics_result': wsobjs_act}

    def get_activities_from_ws(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
        params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_activities_from_ws(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_total_logins_from_ws(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
        params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_total_logins(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_login_stats_from_ws(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
        params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_logins_from_ws(
            params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_ws_stats_from_ws(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
        params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_ws(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_narrative_stats_from_ws(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
        params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_narratives(
            params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_narratives_ws_wsobjs(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
        params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_narratives_ws_wsobjs(
            params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_numObjs_from_ws(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)
        params['minTime'] = datetime.datetime.fromtimestamp(params['minTime'] / 1000)
        params['maxTime'] = datetime.datetime.fromtimestamp(params['maxTime'] / 1000)

        db_ret = self.metrics_dbi.aggr_user_numObjs(
            params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_user_job_states(self, requesting_user, params, token):
        params = self.process_parameters(params)
        if not self.is_admin(requesting_user):
            params['user_ids'] = [requesting_user]

        return self.get_jobdata_from_ws_exec_ujs(params, token)

    def get_jobdata_from_ws_exec_ujs(self, params, token):
        """
        get_jobdata_from_ws_exec_ujs--The original implementation to 
        get data for appcatalog from querying execution_engine,
        catalog, workspace and userjobstate
        ----------------------
        To get the job's 'status', 'complete'=true/false, etc.,
        we can do joining as follows
        --userjobstate.jobstate['_id']==exec_engine.exec_tasks['ujs_job_id']
        To join exec_engine.exec_apps with exec_engine.exec_tasks:
        --exec_apps['app_job_id']==exec_tasks['app_job_id']
        To join exec_engine.exec_apps with ujs.jobstate:
        --ObjectId(exec_apps.app_job_state['job_id'])==jobstate['_id']
        """

        # 0. get the ws_narrative and client_groups data for lookups
        if self.ws_narratives is None:
            self.ws_narratives = self.metrics_dbi.list_ws_narratives()
        if self.client_groups is None:
            self.client_groups = self.get_client_groups_from_cat(token)

        # 1. query dbs to get lists of tasks and jobs
        exec_tasks = self.metrics_dbi.list_exec_tasks(params['minTime'], params['maxTime'])

        exec_apps = self.metrics_dbi.list_exec_apps(params['minTime'], params['maxTime'])

        # subfunc only needs 'app_job_id' and 'app_job_state'
        exec_apps = [{'app_job_id': exec_app.get('app_job_id'),
                      'app_job_state': exec_app.get('app_job_state')} for exec_app in exec_apps]

        ujs_jobs = self.metrics_dbi.list_ujs_results(
            params['user_ids'], params['minTime'], params['maxTime'])
        ujs_jobs = self.convert_isodate_to_millis(
            ujs_jobs, ['created', 'started', 'updated', 'estcompl'])
        return {'job_states': self.join_app_task_ujs(exec_tasks, exec_apps, ujs_jobs)}

    def _get_apptask_list(self, exec_tasks, exec_apps):
        """
        combine/join the apps and tasks to get the app_task_list
        """

        app_task_list = []
        for t in exec_tasks:
            ta = copy.deepcopy(t)
            matched_exec_app = [exec_app for exec_app in exec_apps if
                                exec_app.get('app_job_id') in [ta.get('app_job_id'),
                                                               ta.get('ujs_job_id')]]
            if matched_exec_app:
                ta['job_state'] = matched_exec_app[-1].get('app_job_state')
            app_task_list.append(ta)

        return app_task_list

    def join_app_task_ujs(self, exec_tasks, exec_apps, ujs_jobs):
        """
        combine/join the apps, tasks and jobs lists to get the final return data
        """
        # 1) combine/join the apps and tasks to get the app_task_list
        app_task_list = self._get_apptask_list(exec_tasks, exec_apps)

        # 2) combine/join app_task_list with ujs_jobs list to get the final return data
        ujs_ret = []
        for j in ujs_jobs:
            u_j_s = copy.deepcopy(j)
            u_j_s['job_id'] = u_j_s.pop('_id')
            u_j_s['exec_start_time'] = u_j_s.pop('started', None)
            u_j_s['creation_time'] = u_j_s.pop('created')
            u_j_s['modification_time'] = u_j_s.pop('updated')
            u_j_s['estcompl'] = j.get('estcompl', None)
            u_j_s['time_info'] = [u_j_s.get('creation_time'),
                                  u_j_s.get('modification_time'),
                                  u_j_s['estcompl']]

            if u_j_s.get('authstrat', None) == 'kbaseworkspace':
                u_j_s['wsid'] = u_j_s.get('authparam')

            if u_j_s.get('desc'):
                desc = u_j_s['desc'].split()[-1]
                if '.' in desc:
                    u_j_s['method'] = desc

            # Assuming complete, error and status all exist in the records returned
            if not j.get('error'):
                if j.get('complete'):
                    u_j_s['job_state'] = 'completed'
                elif j.get('status') in ["Initializing", 'queued']:
                    u_j_s['job_state'] = j['status']
                elif j.get('status') in ['canceled', 'cancelled']:
                    u_j_s['job_state'] = 'canceled'
                elif j.get('started'):
                    u_j_s['job_state'] = 'in-progress'
                elif j['created'] == j['updated']:
                    u_j_s['job_state'] = 'not-started'
                elif j['created'] < j['updated'] and not j.get('started'):
                    u_j_s['job_state'] = 'queued'
                else:
                    u_j_s['job_state'] = 'unknown'
            else:
                u_j_s['job_state'] = 'suspended'

            for lat in app_task_list:
                if ObjectId(lat['ujs_job_id']) == j['_id']:
                    if 'job_state' not in u_j_s:
                        u_j_s['job_state'] = lat['job_state']

                    if 'job_input' in lat:
                        u_j_s['job_input'] = lat['job_input']
                        if not u_j_s.get('app_id'):
                            u_j_s['app_id'] = self.parse_app_id(lat)

                        if not u_j_s.get('method'):
                            u_j_s['method'] = self.parse_method(lat)

                        if not u_j_s.get('wsid'):
                            if 'wsid' in lat['job_input']:
                                u_j_s['wsid'] = lat['job_input']['wsid']
                            elif 'params' in lat['job_input']:
                                if 'ws_id' in lat['job_input']['params']:
                                    u_j_s['wsid'] = lat['job_input']['params']['ws_id']
                                if 'workspace' in lat['job_input']['params']:
                                    u_j_s['workspace_name'] = lat[
                                        'job_input']['params']['workspace']
                                elif 'workspace_name' in lat['job_input']['params']:
                                    u_j_s['workspace_name'] = lat['job_input'][
                                        'params']['workspace_name']

                    u_j_s['job_output'] = lat.get('job_output')
                    break

            if not u_j_s.get('app_id') and u_j_s.get('method'):
                u_j_s['app_id'] = u_j_s['method'].replace('.', '/')

            # get the narrative name and version if any
            if u_j_s.get('wsid'):
                n_nm, n_obj = self.map_narrative(u_j_s['wsid'], self.ws_narratives)
                if n_nm != "" and n_obj != 0:
                    u_j_s['narrative_name'] = n_nm
                    u_j_s['narrative_objNo'] = n_obj

            # get some info from the client groups
            u_j_s['client_groups'] = ['njs']  # default client groups to 'njs'
            for clnt in self.client_groups:
                clnt_app_id = clnt['app_id']
                if (str(clnt_app_id).lower() == str(u_j_s.get('app_id')).lower()):
                    u_j_s['client_groups'] = clnt['client_groups']
                    break

            # set the run/running/in_queue time
            if u_j_s['job_state'] == 'completed' or u_j_s['job_state'] == 'suspend':
                u_j_s['finish_time'] = u_j_s['modification_time']
                u_j_s['run_time'] = u_j_s['finish_time'] - u_j_s['exec_start_time']
            elif u_j_s['job_state'] == 'in-progress':
                u_j_s['running_time'] = u_j_s['modification_time'] - u_j_s['exec_start_time']
            elif u_j_s['job_state'] == 'queued':
                u_j_s['time_in_queue'] = u_j_s['modification_time'] - u_j_s['creation_time']

            ujs_ret.append(u_j_s)
        return ujs_ret

    def parse_app_id(self, lat):
        app_id = ''
        if 'app_id' in lat['job_input']:
            if '/' in lat['job_input']['app_id']:
                app_id = lat['job_input']['app_id']
            elif '.' in lat['job_input']['app_id']:
                app_id = lat['job_input']['app_id'].replace('.', '/')
            else:
                app_id = lat['job_input']['app_id']

        return app_id

    def parse_method(self, lat):
        method_id = ''
        if 'method' in lat['job_input']:
            if '.' in lat['job_input']['method']:
                method_id = lat['job_input']['method']
            elif '/' in lat['job_input']['method']:
                method_id = lat['job_input']['method'].replace('/', '.')
            else:
                method_id = lat['job_input']['method']

        return method_id

    def map_narrative(self, wsid, ws_narratives):
        """
        get the narrative name and version
        """
        n_name = ''
        n_obj = 0
        ws_name = ''
        # ws_owner = ''
        for ws in ws_narratives:
            if str(ws['workspace_id']) == str(wsid):
                ws_name = ws['name']
                # ws_owner = ws['username']
                n_name = ws_name
                if not ws.get('meta', None) is None:
                    w_meta = ws['meta']
                    for w_m in w_meta:
                        if w_m['k'] == 'narrative':
                            n_obj = w_m['v']
                        elif w_m['k'] == 'narrative_nice_name':
                            n_name = w_m['v']
                        else:
                            pass
                break
        return (n_name, n_obj)

    def get_ujs_results(self, requesting_user, params, token):
        params = self.process_parameters(params)
        if not self.is_admin(requesting_user):
            # print(requesting_user + ': You have permission to view ONLY your jobs.')
            params['user_ids'] = [requesting_user]

        db_ret = self.metrics_dbi.list_ujs_results(
            params['user_ids'], params['minTime'], params['maxTime'])
        for dr in db_ret:
            dr['_id'] = str(dr['_id'])
        db_ret = self.convert_isodate_to_millis(
            db_ret, ['created', 'started', 'updated', 'estcompl'])

        return {'metrics_result': db_ret}

    def get_exec_apps(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)

        db_ret = self.metrics_dbi.list_exec_apps(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_exec_tasks(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)

        db_ret = self.metrics_dbi.list_exec_tasks(params['minTime'], params['maxTime'])

        return {'metrics_result': db_ret}

    def get_users_from_auth2(self, requesting_user, params, token):
        if not self.is_admin(requesting_user):
            raise ValueError('You do not have permission to view this data.')

        params = self.process_parameters(params)

        db_ret = self.metrics_dbi.aggr_user_details(
            params['user_ids'], params['minTime'], params['maxTime'])
        if len(db_ret) == 0:
            print("No records returned!")
        else:
            db_ret = self.convert_isodate_to_millis(db_ret, ['signup_at', 'last_signin_at'])
        return {'metrics_result': db_ret}

    def process_parameters(self, params):

        params['user_ids'] = params.get('user_ids', [])

        if not isinstance(params['user_ids'], list):
            raise ValueError('Variable user_ids must be a list.')

        if 'kbasetest' in params['user_ids']:
            params['user_ids'].remove('kbasetest')

        epoch_range = params.get('epoch_range')
        if epoch_range:
            if len(epoch_range) != 2:
                raise ValueError('Invalide epoch_range. Size must be 2.')
            start_time, end_time = epoch_range
            if (start_time and end_time):
                start_time = _convert_to_datetime(start_time)
                end_time = _convert_to_datetime(end_time)
            elif (start_time and not end_time):
                start_time = _convert_to_datetime(start_time)
                end_time = start_time + datetime.timedelta(hours=48)
            elif (not start_time and end_time):
                end_time = _convert_to_datetime(end_time)
                start_time = end_time - datetime.timedelta(hours=48)
            else:
                end_time = datetime.datetime.utcnow()
                start_time = end_time - datetime.timedelta(hours=48)
        else:  # set the most recent 48 hours range
            end_time = datetime.datetime.utcnow()
            start_time = end_time - datetime.timedelta(hours=48)

        params['minTime'] = _unix_time_millis_from_datetime(start_time)
        params['maxTime'] = _unix_time_millis_from_datetime(end_time)

        return params

    def get_client_groups_from_cat(self, token):
        """
        get_client_groups_from_cat: Get the client_groups data from Catalog API
        return an array of the following structure (example with data):
        {
            u'app_id': u'assemblyrast/run_arast',
            u'client_groups': [u'bigmemlong'],
            u'function_name': u'run_arast',
            u'module_name': u'AssemblyRAST'},
        }
        """
        # initialize client(s) for accessing other services
        self.cat_client = Catalog(self.catalog_url,
                                  auth_svc=self.auth_service_url, token=token)
        # Pull the data
        client_groups = self.cat_client.get_client_groups({})
        # log("\nClient group example:\n{}".format(pformat(client_groups[0])))

        return client_groups

    def is_admin(self, username):
        return username in self.adminList

    def is_metrics_admin(self, username):
        return username in self.metricsAdmins

    def is_kbstaff(self, username):
        return username in self.kbstaffList

    def convert_isodate_to_millis(self, src_list, dt_list):
        for dr in src_list:
            for dt in dt_list:
                if (dt in dr and isinstance(dr[dt], datetime.datetime)):
                    dr[dt] = _unix_time_millis_from_datetime(dr[dt])  # dr[dt].__str__()
        return src_list
