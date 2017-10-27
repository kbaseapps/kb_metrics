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

        gnf_format = 'genbank'
        ncbi_count_results = []
        feature_printouts = []
        for gn in ncbi_gns:
            gn_feature_counts = self._get_feature_counts(gn['genome_url'], gnf_format, gn['organism_name'])
            ncbi_count_results.append(gn_feature_counts)

            gn_feat_printout = self._printout_feature_counts(gn_feature_counts)
            log(gn_feat_printout)
            feature_printouts.append(gn_feat_printout)

        wsname = params['workspace_name']

        returnVal = {
            "report_ref": None,
            "report_name": None
        }

        if params['create_report'] == 1:
            report_info = self.generate_report(feature_printouts, params)
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

        count_results = []
        feature_printouts = []
        for gn_f in gn_files:
            gn_feature_counts = self._get_feature_counts(gn_f, params['file_format'])
            count_results.append(gn_feature_counts)

            gn_feat_printout = self._printout_feature_counts(gn_feature_counts)
            log(gn_feat_printout)
            feature_printouts.append(gn_feat_printout)

        wsname = params['workspace_name']
        returnVal = {
            "report_ref": None,
            "report_name": None
        }

        if params['create_report'] == 1:
            report_info = self.generate_report(feature_printouts, params)
            returnVal = {
                'report_name': report_info['name'],
                'report_ref': report_info['ref']
            }

        return returnVal

    def generate_report(self, count_info, params):
        # create report
        report_text = 'Summary of genome feature stats:\n\n'
        report_text += ''.join(count_info)

        report_info = self.kbr.create_extended_report({'message': report_text,
                                                  'report_object_name': 'kb_Metrics_report_' + str(uuid.uuid4()),
                                                  'direct_html_link_index': 0,
                                                  'workspace_name': params['workspace_name']
                                                  })
        return report_info

    def _get_feature_counts(self, gn_file, file_format, organism_name=None):
        """
        _get_feature_counts: Given a genome file, count the totals of contigs and features
        and calculates of the mean/median/max length of each feature type
        return the results in the following json structure:
        {
            'organism_name': organism_name,
            'total_contig_count': total_contig_count,
            'total_feature_count': total_feat_count,
            'feature_counts': feature_count_dict,
            'feature_lengths': feature_len_dict
        }
        """
        #log("\nInput file location: {}".format(gn_file))
        if organism_name is None:
            organism_name = os.path.basename(gn_file)

        #download the file from ftp site
        file_resp = self._download_file_by_url( gn_file )

        if os.path.isfile(file_resp):
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


    def _download_file_by_url(self, file_url):
        download_to_dir = os.path.join(self.scratch, str(uuid.uuid4()))
        _mkdir_p(download_to_dir)
        download_file = os.path.join(download_to_dir, 'genome_file.gbff.gz')

        try:
            urllib.urlretrieve(file_url, download_file)
        except HTTPError as e:
            print('The server couldn\'t fulfill the request.')
            print('Error code: ', e.code)
        except URLError as e:
            print('We failed to reach a server.')
            print('Reason: ', e.reason)
        else:# everything is fine
            pass

        return download_file


    def _get_file_content_by_url(self, file_url):
        req = Request(file_url)
        resp = ''
        try:
            resp = urlopen(req)
        except HTTPError as e:
            print('The server couldn\'t fulfill the request.')
            print('Error code: ', e.code)
        except URLError as e:
            print('We failed to reach a server.')
            print('Reason: ', e.reason)
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

