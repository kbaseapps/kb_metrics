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
from pprint import pprint, pformat
from urllib2 import Request, urlopen
from urllib2 import URLError, HTTPError


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
        ncbi_response = self._get_file_by_url( ncbi_summ_ftp_url )
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
        if params.get(self.PARAM_IN_WS, None) is None:
            raise ValueError(self.PARAM_IN_WS + ' parameter is mandatory')

        file_loc = params.get(self.PARAM_IN_GENBANK_FILE_LOCATION, None)
        if file_loc is None:
            raise ValueError(self.PARAM_IN_GENBANK_FILE_LOCATION + ' parameter is mandatory')

        gn_src = params['genome_source'] if 'genome_source' in params else 'refseq'
        gn_domain = params['genome_domain'] if 'genome_domain' in params else 'bacteria'
        gn_cat = params['refseq_category'] if 'refseq_category' in params else 'reference'

        ncbi_gns = self._list_ncbi_genomes(gn_src, gn_domain, gn_cat)

        log("\nInput file location: {}".format(file_loc))
        for gn in ncbi_gns:
            gn_url = os.path.join(gn['ftp_file_dir'], gn['genome_file_name'])
            if gn_url == file_loc:
                log("\nFound the file {} in NCBI".format(gn_url))
                #fetch the file from NCBI ftp site
                file_resp = self._get_file_by_url( file_loc )
                if file_resp != '':
                    #processing the file to get counts
                    log("First line of the genome file:\n{}".format(file_resp.readlines()[0]))

        returnVal = {
            "report_ref": None,
            "report_name": None
        }

        wsname = params['workspace_name']

        return returnVal


    def _get_file_by_url(self, file_url):
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

