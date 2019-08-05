from kb_Metrics.metrics_dbi import MongoMetricsDBI
import threading
import time

def get_config_list(config, config_key):
    list_str = config.get(config_key)
    if not list_str:
        error_msg = 'Required key "{}" not found in config'.format(config_key)
        error_msg += ' of MetricsMongoDBController'
        raise ValueError(error_msg)
    return [x.strip() for x in list_str.split(',') if x.strip()]

class NarrativeCache:
    narrative_map = None
    narrative_map_max_time = None
    id = 0

    def __init__(self, config):
        self.narrative_map_cache = None
        self.metrics_dbi = MongoMetricsDBI(config.get('mongodb-host'),
                                           get_config_list(config, 'mongodb-databases'),
                                           config.get('mongodb-user', ''),
                                           config.get('mongodb-pwd', ''))
        self.lock = threading.Lock()

    def _get(self):
        # Instance version of the cache does not change once set.
        # The lifetime of this cache is an individual request.
        if self.narrative_map_cache is not None:
            return self.narrative_map_cache

        cls = NarrativeCache

        # So we cache the narrative map in this controller instance.
        if cls.narrative_map is None:
            cls.narrative_map = dict()
            ws_narratives = self.metrics_dbi.list_ws_narratives(include_del=True)
            if len(ws_narratives) == 0:
                return cls.narrative_map
        else:
            if cls.narrative_map_max_time is None:
                # This handles the case in which there were NO narratives initially,
                # and thus the max time was not set.
                ws_narratives = self.metrics_dbi.list_ws_narratives(include_del=True)
            else:

                ws_narratives = self.metrics_dbi.list_more_ws_narratives(include_del=True, from_time=cls.narrative_map_max_time)

            if len(ws_narratives) == 0:
                self.narrative_map_cache = cls.narrative_map
                return cls.narrative_map

        max_time = cls.narrative_map_max_time or 0
        for wsnarr in ws_narratives:
            ws_nm = wsnarr.get('name', '')  # workspace_name or ''
            narr_nm = None
            last_saved_at = int(round(wsnarr['last_saved_at'].timestamp() * 1000))
            max_time = max([max_time, last_saved_at])
            # narr_nm = ws_nm  # default narrative_name
            # TODO: this is suspect, because the narrative metadata field
            # should ALWAYS be available. 
            # And we actually no longer need
            # the narrative version; so we can skip this eventually
            # and certainly shouldn't use it since a default object 
            # number doesn't make any sense.
            narr_ver = '1'  # default narrative_objNo
            n_keys = wsnarr['narr_keys']
            n_vals = wsnarr['narr_values']
            for i in range(0, len(n_keys)):
                if n_keys[i] == 'narrative_nice_name':
                    narr_nm = n_vals[i]
                if n_keys[i] == 'narrative':
                    narr_ver = n_vals[i]

            # If the narrative nice name is not present, a temporary
            # narrative, which by convention has the title 'Untitled'
            # in the Narrative UI.
            if narr_nm is None:
                narr_nm = 'Untitled'

            cls.narrative_map[wsnarr['workspace_id']] = (ws_nm, narr_nm, narr_ver)

        cls.narrative_map_max_time = max_time
        self.narrative_map_cache = cls.narrative_map

        return cls.narrative_map

    def get(self):
        """
        _get_narrative_name_map: Fetch the narrative id and name
        (or narrative_nice_name if it exists) into a dictionary
        of {key=ws_id, value=(ws_nm, narr_nm, narr_ver)}
        """

        start = time.time()
        self.id = self.id + 1
        if self.lock.acquire(blocking=True, timeout=5):
            try:
                return self._get()
            finally:
                self.lock.release()
        else:
            elapsed = time.time() - start
            raise Exception('LOCK  [' + str(self.id) + '] Timeout acquiring lock on Narrative Cache after ' + str(elapsed))
