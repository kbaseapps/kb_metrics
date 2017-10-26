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
    PARAM_IN_GENBANK_FILE_LOCATION = 'genbank_file_location'

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
        log("Found {} {} genomes in NCBI {}/{}".format(str(len(ncbi_genomes)), refseq_category, genome_source, division))
        return ncbi_genomes


    def count_genome_features(self, params):
        self._get_feature_counts(params[self.PARAM_IN_GENBANK_FILE_LOCATION])

        wsname = params['workspace_name']
        returnVal = {
            "report_ref": None,
            "report_name": None
        }

        return returnVal


    def _get_feature_counts(self, gn_file):
        #log("\nInput file location: {}".format(gn_file))

        #download the file from ftp site
        file_resp = self._download_file_by_url( gn_file )

        if os.path.isfile(file_resp):
            #processing the file to get counts
            contig_count = 0
            feat_count = 0
            feature_count_dict = dict()
            feature_lens_dict = dict() # for mean, median, max

            gn_f = gzip.open( file_resp, "r" )
            for rec in SeqIO.parse( gn_f, "genbank"):
                contig_count += 1
                for feature in rec.features:
                    flen = feature.__len__()
                    if feature.type in feature_count_dict:
                        feature_count_dict[feature.type] += 1
                        feature_lens_dict[feature.type].append( flen )
                    else:
                        feature_count_dict[feature.type] = 1
                        feature_lens_dict[feature.type] = []
                    feat_count += 1
            gn_f.close()

        log('TOTAL CONTIG COUNT : '  + str(contig_count))
        log('\nFeature count results----:\n' + pformat(feature_count_dict))

        for feat in sorted( feature_lens_dict ):
            if  len( feature_lens_dict[feat] ) > 0:
                log("feature: {} count: {} mean: {} median: {} max: {}".format(
                       feat, feature_count_dict[feat],
                          mean( feature_lens_dict[feat] ),
                                median( feature_lens_dict[feat] ),
                                max( feature_lens_dict[feat] )))
            else:
                log("feature: {} count: {} has no lengths".format(
                       feat, feature_count_dict[feat] ))

        log("TOTAL FEATURE COUNT : " + str(feat_count))


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

        file_loc = params.get(self.PARAM_IN_GENBANK_FILE_LOCATION, None)
        if file_loc is None:
            raise ValueError(self.PARAM_IN_GENBANK_FILE_LOCATION + ' parameter is mandatory')

        params[self.PARAM_IN_GENBANK_FILE_LOCATION] = file_loc
        if params.get('genome_source', None) is None:
            params['genome_source'] = 'refseq'
        if params.get('genome_domain', None) is None:
            params['genome_domain'] = 'bacteria'
        if params.get('refseq_category', None) is None:
            params['refseq_category'] = 'reference'

