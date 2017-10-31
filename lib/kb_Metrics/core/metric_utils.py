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


class metric_utils:
    PARAM_IN_WS = 'workspace_name'
    PARAM_IN_GENBANK_FILES = 'genbank_files'

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

        self.count_dir = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(self.count_dir)


    def _list_ncbi_genomes(self, genome_source='refseq', division='bacteria', refseq_category='reference'):
        """
        list the ncbi genomes of given source/division/category
        return a list of data with structure as below:
        ncbi_genome = {
            "accession": assembly_accession,#column[0]
            "version_status": genome_version_status,#column[10], latest/replaced/suppressed
            "organism_name": organism_name,#column[7]
            "asm_name": assembly_name,#column[15]
            "refseq_category": refseq_category,#column[4]
            "ftp_file_path": ftp_file_path,#column[19]--FTP path: the path to the directory on the NCBI genomes FTP site from which data for this genome assembly can be downloaded.
            "genome_file_name": genome_file_name,#column[19]--File name: the name of the genome assembly file
            [genome_id, genome_version] = accession.split('.');
            "tax_id": tax_id; #column[5]
            "assembly_level": assembly_level; #column[11], Complete/Chromosome/Scaffold/Contig
            "release_level": release_type; #column[12], Majoy/Minor/Patch
            "genome_rep": genome_rep; #column[13], Full/Partial
            "seq_rel_date": seq_rel_date; #column[14], date the sequence was released
            "gbrs_paired_asm": gbrs_paired_asm#column[17]
        }
        """
        ncbi_genomes = []
        ncbi_summ_ftp_url = "ftp://ftp.ncbi.nlm.nih.gov/genomes/{}/{}/{}".format(genome_source, division, "assembly_summary.txt")
        asm_summary = []
        ncbi_response = self._get_file_content_by_url( ncbi_summ_ftp_url )
        if ncbi_response != '':
            asm_summary = ncbi_response.readlines()

            for asm_line in asm_summary:
                if re.match('#', asm_line):
                    continue

                columns = asm_line.split('\t')
                if refseq_category in columns[4]:
                    ncbi_genomes.append({
                        "domain": division,
                        "genome_source": genome_source,
                        "refseq_category": columns[4],
                        "accession": columns[0],
                        "version_status": columns[10],# latest/replaced/suppressed
                        "organism_name": columns[7],
                        "asm_name": columns[15],
                        "ftp_file_dir": columns[19], #path to the directory for download
                        "genome_file_name": "{}_{}".format(os.path.basename(columns[19]),"genomic.gbff.gz"),
                        "genome_url": os.path.join(columns[19], "{}_{}".format(os.path.basename(columns[19]),"genomic.gbff.gz")),
                        "genome_id": columns[0].split('.')[0],
                        "genome_version": columns[0].split('.')[1],
                        "tax_id": columns[5],
                        "assembly_level": columns[11], #Complete/Chromosome/Scaffold/Contig
                        "release_level": columns[12],  #Majoy/Minor/Patch
                        "genome_rep": columns[13], #Full/Partial
                        "seq_rel_date": columns[14], #date the sequence was released
                        "gbrs_paired_asm": columns[17]
                    })
                elif (refseq_category == 'na' and not columns[4]):
                    ncbi_genomes.append({
                        "domain": division,
                        "genome_source": genome_source,
                        "refseq_category": 'na',
                        "accession": columns[0],
                        "version_status": columns[10],# latest/replaced/suppressed
                        "organism_name": columns[7],
                        "asm_name": columns[15],
                        "ftp_file_dir": columns[19], #path to the directory for download
                        "genome_file_name": "{}_{}".format(os.path.basename(columns[19]),"genomic.gbff.gz"),
                        "genome_url": os.path.join(columns[19], "{}_{}".format(os.path.basename(columns[19]),"genomic.gbff.gz    ")),
                        "genome_id": columns[0].split('.')[0],
                        "genome_version": columns[0].split('.')[1],
                        "tax_id": columns[5],
                        "assembly_level": columns[11], #Complete/Chromosome/Scaffold/Contig
                        "release_level": columns[12],  #Majoy/Minor/Patch
                        "genome_rep": columns[13], #Full/Partial
                        "seq_rel_date": columns[14], #date the sequence was released
                        "gbrs_paired_asm": columns[17]
                    })
        log("\nFound {} {} genomes in NCBI {}/{}".format(
                   str(len(ncbi_genomes)), refseq_category, genome_source, division))

        return ncbi_genomes


    def count_ncbi_genome_features(self, input_params):
        params = self.validate_parameters(input_params)

        ncbi_gns = self._list_ncbi_genomes(params['genome_source'],
                                params['genome_domain'], params['refseq_category'])

        returnVal = {
            "report_ref": None,
            "report_name": None
        }

        if len(ncbi_gns) == 0:
            return returnVal

        gnf_format = 'genbank'
        feature_printouts = []
        genome_feature_counts = []

        count_file_full_path = os.path.join(self.count_dir, 'Feature_Counts.json')
        with open(count_file_full_path, 'a') as count_file:
            for gn in ncbi_gns:
                gn_feature_counts = self._get_feature_counts(gn['genome_url'], gnf_format, gn['organism_name'])
                gn_feature_info = self._create_feature_count_json(gn_feature_counts)
                #log("Json structure:\n" + pformat(gn_feature_info))
                count_file.write(pformat(gn_feature_info))
                genome_feature_counts.append(gn_feature_info)

                #log("\nprintouts:\n")
                #gn_feat_printout = self._printout_feature_counts(gn_feature_counts)
                #log(gn_feat_printout)
                #feature_printouts.append(gn_feat_printout)

        if params['create_report'] == 1:
            report_info = self.generate_report(self.count_dir, genome_feature_counts, params)
            returnVal = {
                'report_name': report_info['name'],
                'report_ref': report_info['ref']
            }

        return returnVal


    def count_genome_features(self, params):
        if params.get(self.PARAM_IN_GENBANK_FILES, None) is None:
            raise ValueError(self.PARAM_IN_GENBANK_FILES +
                        ' parameter is mandatory and has at least one string value')
        if type(params[self.PARAM_IN_GENBANK_FILES]) != list:
            raise ValueError(self.PARAM_IN_GENBANK_FILES + ' must be a list')

        gn_files = params[self.PARAM_IN_GENBANK_FILES]
        if params.get('file_format', None) is None:
            params['file_format'] = 'genbank'

        if params.get('create_report', None) is None:
            params['create_report'] = 0

        genome_feature_counts = []
        feature_printouts = []

        count_file_full_path = os.path.join(self.count_dir, 'Feature_Counts.json')
        with open(count_file_full_path, 'a') as count_file:
            for gn_f in gn_files:
                gn_feature_counts = self._get_feature_counts(gn_f, params['file_format'])
                gn_feature_info = self._create_feature_count_json(gn_feature_counts)
                #log("Json structure:\n" + pformat(gn_feature_info))
                count_file.write(pformat(gn_feature_info))
                genome_feature_counts.append(gn_feature_info)

                #count_file.write('******Organism/file name: {}******\nTOTAL CONTIG COUNT={}'.format(
                #gn_feat_printout = self._printout_feature_counts(gn_feature_counts)
                #log(gn_feat_printout)
                #feature_printouts.append(gn_feat_printout)

        returnVal = {
            "report_ref": None,
            "report_name": None
        }

        if params['create_report'] == 1:
            report_info = self.generate_report(self.count_dir, genome_feature_counts, params)
            returnVal = {
                'report_name': report_info['name'],
                'report_ref': report_info['ref']
            }

        return returnVal


    def generate_report(self, count_dir, feat_counts_info, params):
        output_html_files = self._generate_html_report(count_dir, feat_counts_info, params)
        output_json_files = self._generate_output_file_list(count_dir, params)

        # create report
        report_text = 'Summary of genome feature stats:\n\n'

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


    def _get_feature_counts(self, gn_file, file_format, organism_name=None):
        """
        _get_feature_counts: Given a genome file, count the totals of contigs and features
        and calculates of the mean/median/max length of each feature type
        return the results in the following json structure:
        {
            'organism_name': organism_name,
            'ftp_url': gn_file,
            'total_contig_count': total_contig_count,
            'total_feature_count': total_feat_count,
            'feature_counts': feature_count_dict,
            'feature_lengths': feature_len_dict
        }
        """
        if organism_name is None:
            organism_name = os.path.basename(gn_file)

        #download the file from ftp site
        file_resp = self._download_file_by_url( gn_file )

        if os.path.isfile(file_resp) and os.stat(file_resp).st_size > 0:
            #processing the file to get counts
            total_contig_count = 0
            total_feat_count = 0
            feature_count_dict = dict()
            feature_lens_dict = dict() # for mean, median and max of feature lengths

            gn_f = gzip.open( file_resp, "r" )
            for rec in SeqIO.parse( gn_f, file_format):
                total_contig_count += 1
                for feature in rec.features:
                    flen = feature.__len__()
                    if feature.type in feature_count_dict:
                        feature_count_dict[feature.type] += 1
                        feature_lens_dict[feature.type].append( flen )
                    else:
                        feature_count_dict[feature.type] = 1
                        feature_lens_dict[feature.type] = []
                    total_feat_count += 1
            gn_f.close()

        feature_data = {
                'organism_name': organism_name,
                'ftp_url': gn_file,
                'total_contig_count': total_contig_count,
                'total_feature_count': total_feat_count,
                'feature_counts': feature_count_dict,
                'feature_lengths': feature_lens_dict
        }

        return feature_data


    def _printout_feature_counts(self, feature_data):
        """
        _printout_feature_counts: Given the feature_data containing dict structure of feature counts
        and feature lengths, calculates the mean/median/max length of each feature type
        and build the printout the results in an informative way (tentative for now)

        input feature_data is expected to have the following json structure:
        feature_data={
            'organism_name': organism_name,
            'ftp_url': gn_file_ftp_url,
            'total_contig_count': total_contig_count,
            'total_feature_count': total_feat_count,
            'feature_counts': feature_count_dict,
            'feature_lengths': feature_len_dict
        }
        Example of feature_count_dict:
        {'CDS': 574,
         'gene': 617,
         'misc_RNA': 1,
         'misc_feature': 8,
         'rRNA': 3,
         'source': 3,
         'tRNA': 32,
         'variation': 8}
         TOTAL CONTIG COUNT : 3

        Example of stats of feature_len_dict:
            feature: CDS count: 574 mean: 985.596858639 median: 849.0 max: 4224
            feature: gene count: 617 mean: 939.925324675 median: 810.0 max: 4224
            feature: misc_RNA count: 1 has no lengths
            feature: misc_feature count: 8 mean: 922.285714286 median: 959.0 max: 1568
            feature: rRNA count: 3 mean: 1514.0 median: 1514.0 max: 2913
            feature: source count: 3 mean: 7522.0 median: 7522.0 max: 7786
            feature: tRNA count: 32 mean: 76.9032258065 median: 74.0 max: 92
            feature: variation count: 8 mean: 1.0 median: 1.0 max: 1
            TOTAL FEATURE COUNT : 1246
        """
        feature_printout = '******Organism/file name: {}******\nTOTAL CONTIG COUNT={}'.format(
                feature_data['organism_name'], str(feature_data['total_contig_count']))

        feature_count_dict = feature_data['feature_counts']
        feature_printout += '\nFeature count results:\n' + pformat(feature_count_dict)

        feature_lens_dict = feature_data['feature_lengths']
        for feat_type in sorted( feature_lens_dict ):
            feat_count = feature_count_dict[feat_type]
            if  len( feature_lens_dict[feat_type] ) > 0:
                feat_len_mean = mean( feature_lens_dict[feat_type] )
                feat_len_median = median( feature_lens_dict[feat_type] )
                feat_len_max = max( feature_lens_dict[feat_type] )
                feature_printout += "\nfeature: {} count: {} mean: {} median: {} max: {}".format(
                       feat_type, feat_count, feat_len_mean, feat_len_median, feat_len_max )
            else:
                feature_printout += "\nfeature: {} count: {} has no lengths".format(
                       feat_type, feat_count )

        feature_printout += "\n******TOTAL FEATURE COUNT=" + str(feature_data['total_feature_count'])

        return feature_printout


    def _create_feature_count_json(self, feature_data):
        """
        _create_feature_count_json: Given the feature_data containing dict structure of feature counts
        and feature lengths, calculates the mean/median/max length of each feature type
        and save the results into a json structure

        input feature_data is expected to have the following json structure:
        feature_data={
            'organism_name': organism_name,
            'ftp_url': gn_file,
            'total_contig_count': total_contig_count,
            'total_feature_count': total_feat_count,
            'feature_counts': feature_count_dict,
            'feature_lengths': feature_len_dict
        }

        Example of output:
        genome_feature_info={
         'organism_name': 'Saccharomyces cerevisiae S288C',
         'total_contig_count': 17,
         'total_feature_count': 19793,
         'feature_counts_stats': [
             {
                'feature_type': 'CDS',
                'count': 6008,
                'len_stat': {
                     'mean': 1468.13417679,
                     'median': 1200.0,
                     'max': 14733
                }
             },
             {
                'feature_type': 'centromere',
                'count': 64,
                'len_stat': {
                     'mean': 57.9047619048,
                     'median': 25.0,
                     'max': 120
                }
             },
             {
                'feature_type': 'gene',
                'count': 6445,
                'len_stat': {
                     'mean': 1401.65549348,
                     'median': 1131.0,
                     'max': 14733
                }
             },
             {
                'feature_type': 'mRNA',
                'count': 5995,
                'len_stat': {
                     'mean': 1467.81214548,
                     'median': 1197.0,
                     'max': 14733
                }
             },
             {
                'feature_type': 'misc_RNA',
                'count': 10,
                'len_stat': {
                     'mean': 1096.22222222,
                     'median': 361.0,
                     'max': 6858
                }
             },
             {
                'feature_type': 'misc_feature',
                'count': 14,
                'len_stat': {
                     'mean': 955.384615385,
                     'median': 23.0,
                     'max': 3704
                }
             },
             {
                'feature_type': 'mobile_element',
                'count': 50,
                'len_stat': {
                     'mean': 5835.2244898,
                     'median': 5922.0,
                     'max': 6224
                }
             },
             {
                'feature_type': 'ncRNA',
                'count': 102,
                'len_stat': {
                     'mean': 326.594059406,
                     'median': 179.0,
                     'max': 3199
                }
             },
             {
                'feature_type': 'rRNA',
                'count': 14,
                'len_stat': {
                     'mean': 963.461538462,
                     'median': 158.0,
                     'max': 3396
                 }
             },
             {
                'feature_type': 'rep_origin',
                'count': 360,
                'len_stat': {
                     'mean': 300.579387187,
                     'median': 242.0,
                     'max': 2701
                 }
             },
             {
                'feature_type': 'repeat_origin',
                'count': 383,
                'len_stat': {
                     'mean': 293.185863874,
                     'median': 331.5,
                     'max': 715
                 }
             },
             {
                'feature_type': 'source',
                'count': 17,
                'len_stat': {
                     'mean': 745430.4375,
                     'median': 765042.0,
                     'max': 1531933
                }
             },
             {
                'feature_type': 'tRNA',
                'count': 299,
                'len_stat': {
                     'mean': 74.3489932886,
                     'median': 73.0,
                     'max': 100
                }
             },
             {
                'feature_type': 'telomere',
                'count': 32,
                'len_stat': {
                     'mean': 4513.22580645,
                     'median': 5530.0,
                     'max': 13897
                }
             }
         ]
        }
        """
        genome_feature_info = {
            'organism_name': feature_data['organism_name'],
            'ftp_url': feature_data['ftp_url'],
            'total_contig_count': feature_data['total_contig_count'],
            'total_feature_count': feature_data['total_feature_count']
        }

        feat_counts_stats = []
        feat_count_stat = {}

        feature_count_dict = feature_data['feature_counts']
        feature_lens_dict = feature_data['feature_lengths']

        for feat_type in sorted( feature_lens_dict ):
            feat_count = feature_count_dict[feat_type]
            feat_count_stat = {
                'feature_type': feat_type,
                'count': feat_count
            }
            if len( feature_lens_dict[feat_type] ) > 0:
                feat_count_stat['len_stat'] = {
                     'mean': mean( feature_lens_dict[feat_type] ),
                     'median': median( feature_lens_dict[feat_type] ),
                     'max': max( feature_lens_dict[feat_type] )
                }
            else:
                feat_count_stat['len_stat'] = {}

            feat_counts_stats.append(feat_count_stat)

        genome_feature_info['feature_counts_stats'] = feat_counts_stats

        self._write_html( self.count_dir, genome_feature_info )

        return genome_feature_info


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


    def validate_parameters(self, params):
        if params.get(self.PARAM_IN_WS, None) is None:
            raise ValueError(self.PARAM_IN_WS + ' parameter is mandatory')
        #set default parameters
        if params.get('genome_source', None) is None:
            params['genome_source'] = 'refseq'
        if params.get('genome_domain', None) is None:
            params['genome_domain'] = 'bacteria'
        if params.get('refseq_category', None) is None:
            params['refseq_category'] = 'reference'

        if params.get('file_format', None) is None:
            params['file_format'] = 'genbank'

        if params.get('create_report', None) is None:
            params['create_report'] = 0

        return params


    def _generate_output_file_list(self, out_dir, params):
        """
        _generate_output_file_list: zip result files and generate file_links for report
        """
        log('start packing result files')

        output_files = list()

        output_directory = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(output_directory)
        feature_counts = os.path.join(output_directory, '{}_{}_{}_Feature_counts.zip'.format(
                        params['genome_source'], params['genome_domain'], params['refseq_category']))
        self.zip_folder(out_dir, feature_counts)

        output_files.append({'path': feature_counts,
                             'name': os.path.basename(feature_counts),
                             'label': os.path.basename(feature_counts),
                             'description': 'Genome feature counts generated by kb_Metrics'})

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


    def _write_html(self, out_dir, feat_dt, cutoff=2):
        log('\nInput json:\n' + pformat(feat_dt))

        headContent = ("<html><head>\n"
            "<script type='text/javascript' src='https://www.google.com/jsapi'></script>\n"
            "<script type='text/javascript'>\n"
            "  google.load('visualization', '1', {packages:['controls'], callback: drawTable});\n"
            "  google.setOnLoadCallback(drawTable);\n")

        #the table caption
        drawTable = ("\nfunction drawTable() {\n"
            "var data = new google.visualization.DataTable();\n"
            "data.addColumn('string', 'feature_type');\n"
            "data.addColumn('number', 'count');\n"
            "data.addColumn('number', 'feature_mean_len');\n"
            "data.addColumn('number', 'feature_median_len');\n"
            "data.addColumn('number', 'feature_max_len');")

        #the data rows
        row_cnt = 0
        fd_rows = ""
        fd_rows_cut = ""
        for fd in feat_dt['feature_counts_stats']:
            if fd_rows != "":
                fd_rows += ",\n"
            d_rows = []
            d_rows.append("'" + fd['feature_type'] + "'")
            d_rows.append(str(fd['count']))
            if fd['len_stat']:
                d_rows.append(str(fd['len_stat']['mean']))
                d_rows.append(str(fd['len_stat']['median']))
                d_rows.append(str(fd['len_stat']['max']))
            else:
                d_rows.append('NA')
                d_rows.append('NA')
                d_rows.append('NA')

            fd_rows += '[' + ','.join(d_rows) + ']'

            row_cnt += 1
            if row_cnt <= cutoff:
                fd_rows_cut = fd_rows

        drawTable += "\ndata.addRows([\n"

        drawTable_cut = drawTable
        drawTable_cut += fd_rows_cut
        drawTable_cut += "\n]);"

        drawTable += fd_rows
        drawTable += "\n]);"

        #the dashboard, table and search filter
        dash_tab_filter = "\n" \
            "var dashboard = new google.visualization.Dashboard(document.querySelector('#dashboard'));\n" \
            "var stringFilter = new google.visualization.ControlWrapper({\n" \
            "    controlType: 'StringFilter',\n" \
            "    containerId: 'string_filter_div',\n" \
            "    options: {\n" \
            "        filterColumnIndex: 0\n" \
            "    }\n" \
            "});\n" \
            "var table = new google.visualization.ChartWrapper({\n" \
            "    chartType: 'Table',\n" \
            "    containerId: 'table_div',\n" \
            "    options: {\n" \
            "        showRowNumber: true\n" \
            "    }\n" \
            "});\n" \
            "dashboard.bind([stringFilter], [table]);\n" \
            "dashboard.draw(data);\n" \
        "}\n"

        footContent = "</script></head>\n<body>\n"
        footContent += "  <h4>Feature counts stats for {}:</h4>\n".format(feat_dt['organism_name'])
        footContent += "  <div id='dashboard'>\n" \
          "      <div id='string_filter_div'></div>\n" \
          "      <div id='table_div'></div>\n" \
          "  </div>\n" \
          "</body>\n" \
          "</html>"

        html_str = headContent + drawTable + dash_tab_filter + footContent
        #html_str_cut = headContent + drawTable_cut + dash_tab_filter + footContent
        #return {'html_full': html_str, 'html_partial': html_str_cut}
        #log(html_str)

        name_str = feat_dt['organism_name'].replace(' ', '_')
        html_file_path = os.path.join(out_dir, '{}_Feature_counts.html'.format(name_str))

        with open(html_file_path, 'w') as html_file:
                html_file.write(html_str)

        return {'html_file': html_str, 'html_path': html_file_path}


    def _generate_html_report(self, out_dir, feat_counts, params):
        """
        _generate_html_report: generate html report given the json data in feat_counts

        """
        #log('start generating html report')
        html_report = list()

        html_file_path = self._write_html(out_dir, feat_counts[0])
        org_name = feat_counts[0]['organism_name']

        #log(html_file_path['html_file'])
        html_report.append({'path': html_file_path['html_path'],
                            'name': org_name,
                            'label': org_name,
                            'description': 'The feature_counts for one of the orgnism(s) of {}_{}_{}.'.format(
                                        params["genome_source"], params["genome_domain"], params["refseq_category"])
                        })

        return html_report

