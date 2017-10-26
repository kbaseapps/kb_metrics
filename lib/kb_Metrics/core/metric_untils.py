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
import urllib.request
import urllib.parse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


from Workspace.WorkspaceClient import Workspace as Workspace


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


class metrics_utils:

    def __init__(self, config):
        self.workspace_url = config['workspace-url']
        self.callback_url = config['SDK_CALLBACK_URL']
        self.token = config['KB_AUTH_TOKEN']
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
            "accession": assembly_accession;#column[0]
            "version_status": genome_version_status;#column[10], latest/replaced/suppressed
            "organism_name": organism_name;#column[7]
            "asm_name": assembly_name;#column[15]
            "refseq_category": refseq_category; #column[4]
            "ftp_file_path": ftp_file_path;# column[19]--FTP path: the path to the directory on the NCBI genomes FTP site from which data for this genome assembly can be downloaded.
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
        ncbi_ftp_url = "ftp://ftp.ncbi.nlm.nih.gov/genomes/{}/{}/{}".format(genome_source, division."assembly_summary.txt")
        asm_summary = []
        req = urllib.request.Request(ncbi_ftp_url)
        try:
            with urllib.request.urlopen(req) as response:
                asm_summary = response.readlines()
        except HTTPError as e:
            print('The server couldn\'t fulfill the request.')
            print('Error code: ', e.code)
        except URLError as e:
            print('We failed to reach a server.')
            print('Reason: ', e.reason)
        else:# everything is fine
            for asm_line in asm_summary:
                if re.search('/^#.*/$', asm_line):
                    continue

                columns = asm_line.split('\t')
                if refseq_category in columns[4]:
                    ncbi_genomes.append({
                        "domain": division;
                        "genome_source": genome_source;
                        "refseq_category": columns[4];
                        "accession": columns[0];
                        "version_status": columns[10];# latest/replaced/suppressed
                        "organism_name": columns[7];
                        "asm_name": columns[15];
                        "ftp_file_path": column[19]; #--FTP path: the path to the directory on the NCBI genomes FTP site from which data for this genome assembly can be downloaded.
                        "genome_id": columns[0].split('.')[0];
                        "genome_version": columns[0].split('.')[1];
                        "tax_id": columns[5];
                        "assembly_level": columns[11]; #Complete/Chromosome/Scaffold/Contig
                        "release_level": columns[12];  #Majoy/Minor/Patch
                        "genome_rep": columns[13]; #Full/Partial
                        "seq_rel_date": columns[14]; #date the sequence was released
                        "gbrs_paired_asm": columns[17];
                    })

        return ncbi_genomes

    def _count_genome_features(self, ftp_file_path):
