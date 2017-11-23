# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
import requests

from os import environ
try:
    from ConfigParser import ConfigParser  # py2
except:
    from configparser import ConfigParser  # py3

from pprint import pprint, pformat  # noqa: F401

#from biokbase.catalog.Client import Catalog
from biokbase.workspace.client import Workspace as workspaceService
from kb_Metrics.kb_MetricsImpl import kb_Metrics
from kb_Metrics.kb_MetricsServer import MethodContext
from kb_Metrics.authclient import KBaseAuth as _KBaseAuth

from AssemblyUtil.AssemblyUtilClient import AssemblyUtil

class kb_MetricsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        token = environ.get('KB_AUTH_TOKEN', None)
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('kb_Metrics'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)
        cls.ctx.update({'token': token,
                        'user_id': user_id,
                        'provenance': [
                            {'service': 'kb_Metrics',
                             'method': 'please_never_use_it_in_production',
                             'method_params': []
                             }],
                        'authenticated': 1})
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = workspaceService(cls.wsURL)
        cls.serviceImpl = kb_Metrics(cls.cfg)
        cls.scratch = cls.cfg['scratch']
        cls.callback_url = os.environ['SDK_CALLBACK_URL']

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    def getWsClient(self):
        return self.__class__.wsClient

    def getWsName(self):
        if hasattr(self.__class__, 'wsName'):
            return self.__class__.wsName
        suffix = int(time.time() * 1000)
        wsName = "test_kb_Metrics_" + str(suffix)
        ret = self.getWsClient().create_workspace({'workspace': wsName})  # noqa
        self.__class__.wsName = wsName
        return wsName

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    def load_fasta_file(self, filename, obj_name, contents):
        f = open(filename, 'w')
        f.write(contents)
        f.close()
        assemblyUtil = AssemblyUtil(self.callback_url)
        assembly_ref = assemblyUtil.save_assembly_from_fasta({'file': {'path': filename},
                                                              'workspace_name': self.getWsName(),
                                                              'assembly_name': obj_name
                                                              })
        return assembly_ref

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_count_genome_features")
    def test_run_count_genome_features(self):
        # First set input parameters
        m_params =     {
            'workspace_name': self.getWsName(),
            'genbank_file_urls': ['ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/009/605/GCF_000009605.1_ASM960v1/GCF_000009605.1_ASM960v1_genomic.gbff.gz','ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/008/725/GCF_000008725.1_ASM872v1/GCF_000008725.1_ASM872v1_genomic.gbff.gz','ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/009/605/GCF_000009605.1_ASM960v1/GCF_000009605.1_ASM960v1_genomic.gbff.gz'],
            'create_report': 1
        }
        # Second, call your implementation
        ret = self.getImpl().count_genome_features(self.getContext(), m_params)

        # Validate the returned data
        #self.assertEqual(ret[0]['n_initial_contigs'], 3)
        #self.assertEqual(ret[0]['n_contigs_removed'], 1)
        #self.assertEqual(ret[0]['n_contigs_remaining'], 2)


    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_count_genbank_genome_features")
    def test_run_count_genbank_genome_features(self):
        # First set input parameters
        m_params =     {
            'workspace_name': self.getWsName(),
            'genbank_files': [],
            'genome_source': 'refseq',
            'genome_domain': 'bacteria',#'archaea',#'bacteria','plant','fungi'
            'refseq_category': 'reference',#'representative','na',
            'create_report': 1 
        }
        # Second, call your implementation
        ret = self.getImpl().count_ncbi_genome_features(self.getContext(), m_params)


    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_count_ensemblgenome_features")
    def test_run_count_ensemblgenome_features(self):
        # First set input parameters
        m_params =     {
            'workspace_name': self.getWsName(),
            'genbank_files': ['ftp.ensemblgenomes.org/pub/release-37/plants/genbank/corchorus_capsularis/Corchorus_capsularis.CCACVL1_1.0.37.nonchromosomal.dat.gz'],
            'create_report': 0
        }
        # Second, call your implementation
        ret = self.getImpl().count_genome_features(self.getContext(), m_params)


    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_refseq_genome_counts")
    def test_run_refseq_genome_counts(self):
        # First set input parameters
        m_params = {
            'workspace_name': self.getWsName(),
            'genome_source': 'refseq',
            'genome_domain': 'fungi',#'archaea',#'bacteria','plant','fungi'
            'refseq_category': 'reference', #'reference','representative','na',
            'create_report': 1
        }
        # Second, call your implementation
        ret = self.getImpl().refseq_genome_counts(self.getContext(), m_params)


    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_dummy_test")
    def test_run_dummy_test(self):
        m_params = {
            'stats_name': 'user_job_states',
            'workspace_name': self.getWsName(),
            'create_report': 0
        }
        # Second, call your implementation
        ret = self.getImpl().dummy_test0(self.getContext(), m_params)
        print(pformat(ret[0]))

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    @unittest.skip("skipped test_run_report_metrics")
    def test_run_report_metrics(self):
        m_params = {
            'stats_name': 'exec_stats',#'exec_aggr_stats','exec_aggr_table','user_job_states'
            'workspace_name': self.getWsName(),
            'create_report': 0
        }
        # Second, call your implementation
        ret = self.getImpl().report_metrics(self.getContext(), m_params)
        print(pformat(ret[0]))


    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    # Uncomment to skip this test
    #@unittest.skip("skipped test_run_get_app_metrics")
    def test_run_get_app_metrics(self):
        m_params = {
            'user_ids': [],
            'time_range':(u'2017-08-27T17:29:37+0000', u'2017-11-27T17:29:42+0000'),#[u'2017-10-27T17:29:37+0000', u'2017-10-27T17:29:42+0000'],
            'job_stage': 'complete'#'created', 'started', 'complete', 'canceled', 'error' or 'all'
        }
        # Second, call your implementation
        ret = self.getImpl().get_app_metrics(self.getContext(), m_params)
        print(pformat(ret[0]['job_states'][0]))

