import time
import json
import os
import re
import copy
import uuid
import subprocess
import shutil
import sys
import zipfile
import gzip
from pprint import pprint, pformat
from urllib2 import Request, urlopen
from urllib2 import URLError, HTTPError
import urllib
import errno

from Bio import Entrez, SeqIO
from numpy import median, mean, max

from Workspace.WorkspaceClient import Workspace as Workspace
from KBaseReport.KBaseReportClient import KBaseReport
from Catalog.CatalogClient import Catalog
#from biokbase.catalog.Client import Catalog

def log(message, prefix_newline=False):
    """Logging function, provides a hook to suppress or redirect log messages."""
    print(('\n' if prefix_newline else '') + '{0:.2f}'.format(time.time()) + ': ' + str(message))


def _mkdir_p(path):
    """
    _mkdir_p: make directory for given path
    """
    if not path:
        return
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class report_utils:
    PARAM_IN_WS = 'workspace_name'

    def __init__(self, config, provenance):
        self.workspace_url = config['workspace-url']
        self.callback_url = os.environ['SDK_CALLBACK_URL']
        self.token = os.environ['KB_AUTH_TOKEN']
        self.provenance = provenance

        self.scratch = os.path.join(config['scratch'], str(uuid.uuid4()))
        _mkdir_p(self.scratch)

        if 'shock-url' in config:
            self.shock_url = config['shock-url']
        if 'handle-service-url' in config:
            self.handle_url = config['handle-service-url']

        self.ws_client = Workspace(self.workspace_url, token=self.token)
        self.kbr = KBaseReport(self.callback_url)
        #self.cat = Catalog(self.callback_url)
        self.cat = Catalog('https://kbase.us/services/catalog', auth_svc='https://kbase.us/services/auth/')

        self.count_dir = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(self.count_dir)


    def create_stats_report(self, params):
        if params.get(self.PARAM_IN_WS, None) is None:
           raise ValueError(self.PARAM_IN_WS + ' parameter is mandatory')

        if params.get('stats_name', None) is None:
            raise ValueError('Variable stats_name' + ' parameter is mandatory')

        stats_name = params['stats_name']

        cat_calls = {
            'exec_stats': 'get_exec_stats_from_cat',
            'exec_aggr_stats': 'get_exec_aggrStats_from_cat',
            'exec_aggr_table': 'get_exec_aggrTable_from_cat'
        }
        ret_stats = []
        if stats_name == 'exec_stats':
            ret_stats = self.get_exec_stats_from_cat()
        elif stats_name == 'exec_aggr_stats':
            ret_stats = self.get_exec_aggrStats_from_cat()
        elif stats_name == 'exec_aggr_table':
            ret_stats = self.get_exec_aggrTable_from_cat()

        returnVal = {
            "report_ref": None,
            "report_name": None
        }

        if len(ret_stats) == 0:
            return returnVal

        col_caps = ['module_name', 'full_app_id', 'number_of_calls', 'number_of_errors',
                        'type', 'time_range', 'total_exec_time', 'total_queue_time']
        if params['create_report'] == 1:
            report_info = self.generate_report(self.count_dir, ret_stats, params)
            #report_info = self.generate_report(self.count_dir, raw_stats, params)
            #report_info = self.generate_report(self.count_dir, aggr_stats, params, col_caps)

            returnVal = {
                'report_name': report_info['name'],
                'report_ref': report_info['ref']
            }

        return returnVal


    def get_exec_stats_from_cat(self):
        """
        get_exec_stats_from_cat: Get stats on completed jobs
        return an array of the following structure (example with data):
        {
             u'app_id': u'describe_rnaseq_experiment',
             u'app_module_name': u'KBaseRNASeq',
             u'creation_time': 1456863947.568,
             u'exec_start_time': 1456863953.739,
             u'finish_time': 1456863955.138,
             u'func_module_name': u'KBaseRNASeq',
             u'func_name': u'SetupRNASeqAnalysis',
             u'git_commit_hash': u'5de844e7303a8a30a94d4ca40f2b341439b8bb3c',
             u'is_error': True,
             u'user_id': u'srividya22'
        }
        """
        # Pull the data
        log("Fetching the data from Catalog API...")
        #raw_stats = self.cat.get_exec_raw_stats({})
        #raw_stats = self.cat.get_exec_raw_stats({},{'begin': 1510558000, 'end': 1510680000})
        raw_stats = self.cat.get_exec_raw_stats({},{'begin': 1510558000, 'end': 1510568000})

        # Convert time from seconds to hours
        for elem in raw_stats:
            tc = elem['creation_time']
            ts = elem['exec_start_time']
            tf = elem['finish_time']
            elem['creation_time'] = tc / 3600
            elem['exec_start_time'] = ts /3600
            elem['finish_time'] = tf / 3600
            elem['queued_time'] = elem['exec_start_time'] - elem['creation_time']
            elem['run_time'] = elem['finish_time'] - elem['exec_start_time']

        log(pformat(raw_stats[0]))

        return raw_stats


    def get_exec_aggrTable_from_cat(self):
        """
        get_exec_stats_from_cat: Get stats on completed jobs
        return an array of the following structure (example with data):
        {
             u'app': u'kb_uploadmethods/import_sra_as_reads_from_web',
             u'func': u'import_sra_from_web',
             u'func_mod': u'kb_uploadmethods',
             u'n': 5,
             u'user': u'umaganapathyswork'
        }
        """
        # Pull the data
        log("Fetching the data from Catalog API...")
        aggr_tab = self.cat.get_exec_aggr_table({})

        log(pformat(aggr_tab[0]))

        return aggr_tab


    def get_exec_aggrStats_from_cat(self):
        """
        get_exec_aggr_from_cat: Get stats on aggregated execution results of KBase apps
        return an array of the following structure (example with data):
        {
             u'full_app_id': u'KBaseRNASeq/describe_rnaseq_experiment',
             u'module_name': u'KBaseRNASeq',
             u'number_of_calls': 689,
             u'number_of_errors': 117,
             u'time_range': u'*',
             u'total_exec_time': 10.807103612158034,
             u'total_queue_time': 127.90380222181479,
             u'type': u'a'
        }
        """
        # Pull the data
        aggr_stats = self.cat.get_exec_aggr_stats({})

        # Convert time from seconds to hours
        for kb_mod in aggr_stats:
            te = kb_mod['total_exec_time']
            tq = kb_mod['total_queue_time']
            kb_mod['total_exec_time'] = te/3600
            kb_mod['total_queue_time'] = tq/3600

        log(pformat(aggr_stats[0]))

        return aggr_stats


    def get_module_stats_from_cat(self):
        """
        get_module_stats_from_cat: Get stats on Modules
        """
        now = time.time()
        kb_modules = dict()
        for kb_module in self.cat.list_basic_module_info({'include_unreleased':True}):
            name = kb_module['module_name']
            v = self.cat.get_module_info({'module_name':name})['beta']
            vers = self.cat.list_released_module_versions({'module_name':name})
            s = 'b'
            if len(vers)>0:
                v = vers[0]
                s = 'r'
            if v is None:
                continue
            ct = len(v['narrative_methods'])
            days = (v['timestamp']/1000)/3600/24
            #print '%-40s %3d %3d' %(kb_module['module_name'],days,ct)
            kb_modules['%s:%d:%s' %(name,ct,s)] = days
        #log(pformat(kb_modules))

        # Generate time based summaries
        sorted_x = sorted(kb_modules, key=lambda i: int(kb_modules[i]))
        mods = dict()
        apps = dict()
        rmods = dict()
        rapps = dict()
        for bucket in range(184,300):
            mods[bucket] = 0
            apps[bucket] = 0
            rmods[bucket] = 0
            rapps[bucket] = 0
        for m in sorted_x:
            (name,ct,s) = m.split(':')
            d = kb_modules[m]
            bucket = int(d/91.25)
            if bucket not in mods:
                mods[bucket] = 0
                apps[bucket] = 0
                rmods[bucket] = 0
                rapps[bucket] = 0
            mods[bucket] += 1
            apps[bucket] += int(ct)
            if s == 'r':
                rmods[bucket] += 1
                rapps[bucket] += int(ct)
            #print '%-40s %3d %3d' %(name,int(ct),kb_modules[m])

        # Modules by Quarter
        tmods = 0
        tapps = 0
        trmods = 0
        trapps = 0
        Q = 1
        Y =16
        labels = dict()
        bucket = 184
        for year in range(16,21):
            for quarter in range(1,5):
                labels[bucket] = 'Q%d-%2d' % (quarter,year)
                bucket += 1
        for b in range(184,191):
            tmods += mods[b]
            tapps += apps[b]
            trmods += rmods[b]
            trapps += rapps[b]
            print '%5s %3d %3d       %3d %3d   %3d %3d' %(labels[b],tmods, tapps,trmods,trapps,tmods-trmods,tapps-trapps)

        return kb_modules


    def generate_report(self, count_dir, data_info, params, col_caps=None):
        if col_caps is None:
            output_html_files = self._generate_html_report(count_dir, data_info)
        else:
            output_html_files = self._generate_html_report(count_dir, data_info, col_caps)

        output_json_files = self._generate_output_file_list(count_dir)

        # create report
        report_text = 'Summary of {} stats:\n\n'.format(params['stats_name'])

        report_info = self.kbr.create_extended_report({
                        'message': report_text,
                        'report_object_name': 'kb_Metrics_report_' + str(uuid.uuid4()),
                        'file_links': output_json_files,
                        'direct_html_link_index': 0,
                        'html_links': output_html_files,
                        'html_window_height': 366,
                        'workspace_name': params[self.PARAM_IN_WS]
                      })

        return report_info


    def _download_file_by_url(self, file_url):
        download_to_dir = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(download_to_dir)
        download_file = os.path.join(download_to_dir, 'genome_file.gbff.gz')

        try:
            urllib.urlretrieve(file_url, download_file)
        except HTTPError as e:
            log('The server couldn\'t download {}.'.format(file_url))
            log('Error code: ', e.code)
        except URLError as e:
            log('We failed to reach the server to download {}.'.format(file_url))
            log('Reason: ', e.reason)
        except IOError as e:
            log('Caught IOError when downloading {}.'.format(file_url))
            log('Error number: ', e.errno)
            log('Error code: ', e.strerror)
            if e.errno == errno.ENOSPC:
                log('No space left on device.')
            elif e.errno == 110:#[Errno ftp error] [Errno 110] Connection timed out
                log('Connection timed out, trying to urlopen!')
                try:
                    fh = urllib2.urlopen(file_url)
                    data = fh.read()
                    with open(download_file, "w") as dfh:
                        dfh.write(data)
                except:
                    log('Connection timed out, urlopen try also failed!')
                else:
                    pass
        else:# everything is fine
            pass

        return download_file


    def _get_file_content_by_url(self, file_url):
        req = Request(file_url)
        resp = ''
        try:
            resp = urlopen(req)
        except HTTPError as e:
            log('The server couldn\'t fulfill the request to download {}.'.format(file_url))
            log('Error code: ', e.code)
        except URLError as e:
            log('We failed to reach a server to download {}.'.format(file_url))
            log('Reason: ', e.reason)
        else:# everything is fine
            pass

        return resp


    def _generate_output_file_list(self, out_dir):
        """
        _generate_output_file_list: zip result files and generate file_links for report
        """
        log('start packing result files')

        output_files = list()

        output_directory = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(output_directory)
        output_file_path = os.path.join(output_directory, 'output_files.zip')
        self.zip_folder(out_dir, output_file_path)

        output_files.append({'path': output_file_path,
                             'name': os.path.basename(output_file_path),
                             'label': os.path.basename(output_file_path),
                             'description': 'Output files generated by kb_Metrics'})

        return output_files


    def zip_folder(self, folder_path, output_path):
        """Zip the contents of an entire folder (with that folder included in the archive).
        Empty subfolders could be included in the archive as well if the commented portion is used.
        """
        with zipfile.ZipFile(output_path, 'w',
                             zipfile.ZIP_DEFLATED,
                             allowZip64=True) as ziph:
            for root, folders, files in os.walk(folder_path):
                # Include all subfolders, including empty ones.
                #for folder_name in folders:
                #    absolute_path = os.path.join(root, folder_name)
                #    relative_path = os.path.join(os.path.basename(root), folder_name)
                #    print "Adding {} to archive.".format(absolute_path)
                #    ziph.write(absolute_path, relative_path)
                for f in files:
                    absolute_path = os.path.join(root, f)
                    relative_path = os.path.join(os.path.basename(root), f)
                    #print "Adding {} to archive.".format(absolute_path)
                    ziph.write(absolute_path, relative_path)

        print "{} created successfully.".format(output_path)

    def _write_headContent(self):
        """_write_headConten: returns the very first portion of the html file
        """
        head_content = ("<html><head>\n"
            "<script type='text/javascript' src='https://www.google.com/jsapi'></script>\n"
            "<script type='text/javascript'>\n"
            "// Load the Visualization API and the controls package.\n"
            "  google.load('visualization', '1', {packages:['controls'], callback: drawDashboard});\n"
            "  google.setOnLoadCallback(drawDashboard);\n")

        return head_content

    def _write_callback_function(self, input_dt, col_caps=None):
        """
        _write_callback_function: write the callback function according to the input_dt and column captions
        """
        callback_func = ("\nfunction drawDashboard() {\n"
            "var data = new google.visualization.DataTable();\n")

        #table column captions
        if col_caps is None:
            col_caps = input_dt[0].keys()

        cols = []
        for i, col in enumerate(col_caps):
            for k in input_dt[0].keys():
                if col == k:
                    col_type = type(input_dt[0][k]).__name__
                    if (col_type == 'str' or col_type == 'unicode'):
                        col_type = 'string'
                    else:
                        col_type = 'number'
                    callback_func += "data.addColumn('" + col_type + "','" + k + "');\n"
                    cols.append( col )

        #data rows
        dt_rows = ""
        for dt in input_dt:
            if dt_rows != "":
                dt_rows += ",\n"
            d_rows = []
            for j, c in enumerate( cols ):
                d_type = type(dt[c]).__name__
                if (d_type == 'str' or d_type == 'unicode'):
                    if dt[c] is None:
                        d_rows.append('"None"')
                    else:
                        d_rows.append('"' + dt[c] + '"')
                else:
                    if dt[c] is None:
                        d_rows.append('"None"')
                    else:
                        d_rows.append(str(dt[c]))

            dt_rows += '[' + ','.join(d_rows) + ']'

        callback_func += "\ndata.addRows([\n"
        callback_func += dt_rows
        callback_func += "\n]);"

        return callback_func


    def _write_charts(self):
        cat_picker = ("\nvar categoryPicker = new google.visualization.ControlWrapper({\n"
                "controlType: 'CategoryFilter',\n"
                "containerId: 'cat_picker_div',\n"
                "options: {\n"
                "//filterColumnIndex: 0, // filter by this column\n"
                "filterColumnLabel: 'user_id',\n"
                "ui: {\n"
                "    caption: 'Choose a value',\n"
                "    sortValues: true,\n"
                "    allowNone: true,\n"
                "    allowMultiple: true,\n"
                "    allowTyping: true\n"
                "  }\n"
                "},\n"
                "// Define an initial state, i.e. a set of metrics to be initially selected.\n"
                "//state: {'selectedValues': ['KBaseRNASeq', 'MEGAHIT', 'fba_tools']}\n"
                "state: {'selectedValues': ['qzhang', 'srividya22']\n"
            "});\n")

        num_slider1 = ("\n//Create a range slider, passing some options\n"
            "var hourRangeSlider = new google.visualization.ControlWrapper({\n"
                "'controlType': 'NumberRangeFilter',\n"
                "'containerId': 'number_filter_div2',\n"
                "'options': {\n"
                "'filterColumnLabel': 'total_exec_time',\n"
                "'minValue': 0,\n"
                "'maxValue': 720\n"
                "},\n"
                "'state': {'lowValue': 10, 'highValue': 360}\n"
                "});\n")

        line_chart = ("var lineChart = new google.visualization.ChartWrapper({\n"
                "'chartType' : 'Line',\n"
                "'containerId' : 'line_div',\n"
                "'options': {\n"
                "'width': 600,\n"
                "'height': 300,\n"
                "'hAxis': {\n"
                "'title': 'full app id'\n"
                "},\n"
                "'vAxis': {\n"
                "'title': 'total execution time'\n"
                "},\n"
                "'chartArea': {'left': 15, 'top': 25, 'right': 0, 'bottom': 15}\n"
                "},\n"
                "'view': {'columns': [1, 6]}\n"
                "});\n")

        num_slider2 = ("\n//Create a range slider, passing some options\n"
                "var callsRangeSlider = new google.visualization.ControlWrapper({\n"
                "'controlType': 'NumberRangeFilter',\n"
                "'containerId': 'number_filter_div1',\n"
                "'options': {\n"
                "'filterColumnLabel': 'number_of_calls',\n"
                "'minValue': 1,\n"
                "'maxValue': 20000\n"
                "},\n"
                "'state': {'lowValue': 1000, 'highValue': 10000}\n"
                "});\n")

        pie_chart = ("\n//Create a pie chart, passing some options\n"
                "var pieChart = new google.visualization.ChartWrapper({\n"
                "'chartType': 'PieChart',\n"
                "'containerId': 'chart_div',\n"
                "'options': {\n"
                "'width': 300,\n"
                "'height': 300,\n"
                "'pieSliceText': 'value', //'label',\n"
                "'legend': 'none',\n"
                "'chartArea': {'left': 15, 'top': 25, 'right': 0, 'bottom': 15},\n"
                "'title': 'Set your chart title, e.g., Number of calls per module'\n"
                "},\n"
                "// The pie chart will use the columns 'module_name' and 'number_of_calls'\n"
                "// out of all the available ones.\n"
                "'view': {'columns': [1, 2]}\n"
                "});\n")

        tab_chart = ("\n//create a list of columns for the table chart\n"
            "var filterColumns = [{\n"
            "// this column aggregates all of the data into one column for use with the string filter\n"
            "type: 'string',\n"
            "calc: function (dt, row) {\n"
            "for (var i = 0, vals = [], cols = dt.getNumberOfColumns(); i < cols; i++) {\n"
            "    vals.push(dt.getFormattedValue(row, i));\n"
            "}\n"
            "return vals.join('\\n');\n"
            "}\n"
            "}];\n"
            "var tab_columns = [];\n"
            "for (var i = 0, cols = data.getNumberOfColumns(); i < cols; i++) {\n"
            "    filterColumns.push(i);\n"
            "    tab_columns.push(i + 1);\n"
            "}\n"
            "var stringFilter = new google.visualization.ControlWrapper({\n"
            "    controlType: 'StringFilter',\n"
            "    containerId: 'string_filter_div',\n"
            "    options: {\n"
            "        filterColumnIndex: 0,\n"
            "        matchType: 'any',\n"
            "        caseSensitive: false,\n"
            "        ui: {\n"
            "            label: 'Search table:'\n"
            "           }\n"
            "    },\n"
            "    view: {\n"
            "               columns: filterColumns\n"
            "          }\n"
            "});\n"
            "var table = new google.visualization.ChartWrapper({\n"
            "    chartType: 'Table',\n"
            "    containerId: 'table_div',\n"
            "    options: {\n"
            "        showRowNumber: true,\n"
            "        page: 'enable',\n"
            "        pageSize: 20\n"
            "    },\n"
            "    view: {\n"
            "               columns: tab_columns\n"
            "          }\n"
            "});\n")

        return cat_picker + num_slider1 + line_chart + num_slider2 + pie_chart + tab_chart


    def _write_dashboard(self):
        """
        _write_dashboard: writes the dashboard layout and bind controls with charts
        """
        #the dashboard components (table, charts and filters)
        dash_components = self._write_charts()
        dashboard = ("\n"
            "var dashboard = new google.visualization.Dashboard(document.querySelector('#dashboard_div'));\n"
            "dashboard.bind([categoryPicker], [pieChart, lineChart],[table]);\n"
            "dashboard.bind([callsRangeSlider], [pieChart]);\n"
            "dashboard.bind([hourRangeSlider], [lineChart]);\n"
            "dashboard.bind([stringFilter], [table]);\n"
            "dashboard.draw(data);\n"
        "}\n")

        return dash_components + dashboard

    def _write_footcontent(self):
        report_title = "Report_title_here"
        footContent = "</script></head>\n<body>\n"
        footContent += "<h4>" + report_title + "</h4>\n"
        footContent += "  <div id='dashboard_div'>\n" \
                "<div id='cat_picker_div'></div>\n" \
                "<div style='display: inline-block'>\n" \
                "<div id='number_filter_div1'></div>\n" \
                "<div id='chart_div'></div>\n" \
                "</div>\n" \
                "<div style='display: inline-block'>\n" \
                "<div id='number_filter_div2'></div>\n" \
                "<div id='line_div'></div>\n" \
                "</div>\n" \
                "<div id='string_filter_div'></div>\n" \
                "<div id='table_div'></div>\n" \
                "</div>\n" \
                "</body></html>"

        return footContent

    def _write_html(self, out_dir, input_dt, col_caps=None):
        #log('\nInput json:\n' + pformat(input_dt))

        headContent = self._write_headContent()

        if col_caps is None:
            callbackFunc = self._write_callback_function(input_dt)
        else:
            callbackFunc = self._write_callback_function(input_dt, col_caps)

        dashboard = self._write_dashboard()

        footContent = self._write_footcontent()

        html_str = headContent + callbackFunc + dashboard + footContent
        log(html_str)

        html_file_path = os.path.join(out_dir, 'report_charts.html')

        with open(html_file_path, 'w') as html_file:
                html_file.write(html_str)

        return {'html_file': html_str, 'html_path': html_file_path}


    def _generate_html_report(self, out_dir, dt_info, col_caps=None):
        """
        _generate_html_report: generate html report given the json data in feat_counts

        """
        #log('start generating html report')
        html_report = list()

        if col_caps is None:
            html_file_path = self._write_html(out_dir, dt_info)
        else:
            html_file_path = self._write_html(out_dir, dt_info, col_caps)

        file_title = 'Report with charts'

        #log(html_file_path['html_file'])
        html_report.append({'path': html_file_path['html_path'],
                            'name': file_title,
                            'label': file_title,
                            'description': 'The report with charts'
                        })

        return html_report



