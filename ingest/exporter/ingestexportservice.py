#!/usr/bin/env python
"""
desc goes here
"""
__author__ = "jupp"
__license__ = "Apache 2.0"


import json
import logging
import os
import sys
import uuid

from optparse import OptionParser
from urllib.parse import urljoin

import ingest.api.dssapi as dssapi
import ingest.api.ingestapi as ingestapi
import ingest.api.stagingapi as stagingapi

DEFAULT_INGEST_URL = os.environ.get('INGEST_API', 'http://api.ingest.dev.data.humancellatlas.org')
DEFAULT_STAGING_URL = os.environ.get('STAGING_API', 'http://staging.dev.data.humancellatlas.org')
DEFAULT_DSS_URL = os.environ.get('DSS_API', 'http://dss.dev.data.humancellatlas.org')

BUNDLE_SCHEMA_BASE_URL = os.environ.get('BUNDLE_SCHEMA_BASE_URL', 'https://schema.humancellatlas.org')


class IngestExporter:
    def __init__(self, options=None):
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(format=format)
        self.logger = logging.getLogger(__name__)

        self.dryrun = options.dry if options and options.dry else False
        self.outputDir = options.output if options and options.output else None

        self.ingestUrl = options.ingest if options and options.ingest else os.path.expandvars(DEFAULT_INGEST_URL)

        self.stagingUrl = options.staging if options and options.staging else os.path.expandvars(DEFAULT_STAGING_URL)
        self.dssUrl = options.dss if options and options.dss else os.path.expandvars(DEFAULT_DSS_URL)
        self.schema_url = os.path.expandvars(BUNDLE_SCHEMA_BASE_URL)

        self.staging_api = stagingapi.StagingApi()
        self.dss_api = dssapi.DssApi()
        self.ingest_api = ingestapi.IngestApi(self.ingestUrl)

    def export_bundle(self, submission_uuid, process_uuid):
        saved_bundle_uuid = None

        if not self.dryrun and not self.staging_api.hasStagingArea(submission_uuid):
            error_message = "Can't do export as no upload area has been created."
            raise NoUploadAreaFoundError(error_message)

        self.logger.info('Export bundle for process with UUID ' + process_uuid)

        self.logger.info('Retrieving all process information...')

        process = self.ingest_api.getEntityByUuid('processes', process_uuid)
        process_info = self.get_all_process_info(process)

        self.logger.info('Generating bundle files...')
        submission = self.ingest_api.getEntityByUuid('submissionEnvelopes', submission_uuid)
        is_indexed = submission['triggersAnalysis']

        metadata_by_type = self.get_metadata_by_type(process_info)
        files_by_type = self.prepare_metadata_files(metadata_by_type, is_indexed)

        links = self.bundle_links(process_info.links)
        links_file_uuid = str(uuid.uuid4())
        files_by_type['links'] = list()
        files_by_type['links'].append({
            'content': links,
            'content_type': '"metadata/{0}"'.format('links'),
            'indexed': True,
            'dss_filename': 'links.json',
            'dss_uuid': links_file_uuid,
            'upload_filename': 'links_' + links_file_uuid + '.json'
        })

        # restructure bundle manifest
        bundle_manifest = self.create_bundle_manifest(submission_uuid, files_by_type)

        self.logger.info('Generating bundle files...')

        if self.dryrun:
            self.logger.info('Export is using dry run mode.')
            self.logger.info('Dumping bundle files...')

            for metadata_type in ['project', 'biomaterial', 'process', 'protocol', 'file', 'links']:
                for metadata_doc in files_by_type[metadata_type]:
                    bundle_file = metadata_doc
                    filename = bundle_file['upload_filename']
                    content = bundle_file['content']
                    self.dump_to_file(json.dumps(content, indent=4), filename, output_dir=bundle_manifest.bundleUuid)

        else:
            self.logger.info('Uploading metadata files...')
            self.upload_metadata_files(submission_uuid, files_by_type)

            self.logger.info('Saving files in DSS...')
            bundle_uuid = bundle_manifest.bundleUuid

            metadata_files = self.get_metadata_files(files_by_type)
            data_files = self.get_data_files(metadata_by_type['file'])
            bundle_files = metadata_files + data_files

            bundle_manifest.dataFiles = list()
            bundle_manifest.dataFiles = [data_file['dss_uuid'] for data_file in data_files]

            created_files = self.put_files_in_dss(bundle_uuid, bundle_files)

            self.logger.info('Saving bundle in DSS...')
            self.put_bundle_in_dss(bundle_uuid, created_files)

            self.logger.info('Saving bundle manifest...')
            self.ingest_api.createBundleManifest(bundle_manifest)

            saved_bundle_uuid = bundle_manifest.bundleUuid

            self.logger.info('Bundle ' + bundle_uuid + ' was successfully created!')

        return saved_bundle_uuid

    def get_metadata_by_type(self, process_info: 'ProcessInfo') -> dict:
        #  given a ProcessInfo, pull out all the metadata and return as a map of UUID->metadata documents
        simplified = dict()
        simplified['process'] = dict(process_info.derived_by_processes)
        simplified['biomaterial'] = dict(process_info.input_biomaterials)
        simplified['protocol'] = dict(process_info.protocols)
        simplified['file'] = dict(process_info.derived_files)
        simplified['file'].update(process_info.input_files)

        simplified['project'] = dict()
        simplified['project'][process_info.project['uuid']['uuid']] = process_info.project

        return simplified

    def get_all_process_info(self, process):
        process_info = ProcessInfo()
        process_info.input_bundle = self.get_input_bundle(process)

        process_info.project = self.get_project_info(process)

        if not process_info.project:  # get from input bundle
            project_uuid_lists = list(process_info.input_bundle['fileProjectMap'].values())

            if len(project_uuid_lists) == 0 and len(project_uuid_lists[0]) == 0:
                raise Error('Input bundle manifest has no list of project uuid.')  # very unlikely to happen

            project_uuid = project_uuid_lists[0][0]
            process_info.project = self.ingest_api.getProjectByUuid(project_uuid)

        self.recurse_process(process, process_info)

        return process_info

    def get_project_info(self, process):
        projects = list(self.ingest_api.getRelatedEntities('projects', process, 'projects'))

        if len(projects) > 1:
            raise MultipleProjectsError('Can only be one project in bundle')

        # TODO add checking for project only on an assay process
        # TODO an analysis process may have no link to a project

        if len(projects) > 0:
            return projects[0]

        return None

    # get all related info of a process
    def recurse_process(self, process, process_info):
        chained_processes = list(self.ingest_api.getRelatedEntities('chainedProcesses', process, 'processes'))

        is_wrapper = len(chained_processes) > 0

        # don't include wrapper processes in process bundle
        if is_wrapper:
            for chained_process in chained_processes:
                uuid = chained_process['uuid']['uuid']
                process_info.derived_by_processes[uuid] = chained_process
        else:
            uuid = process['uuid']['uuid']
            process_info.derived_by_processes[uuid] = process

        # get all derived by processes using input biomaterial and input files
        derived_by_processes = []

        # wrapper process has the links to input biomaterials and derived files to check if a process is an assay
        input_biomaterials = list(self.ingest_api.getRelatedEntities('inputBiomaterials', process, 'biomaterials'))
        for input_biomaterial in input_biomaterials:
            uuid = input_biomaterial['uuid']['uuid']
            process_info.input_biomaterials[uuid] = input_biomaterial
            derived_by_processes.extend(
                self.ingest_api.getRelatedEntities('derivedByProcesses', input_biomaterial, 'processes'))

        input_files = list(self.ingest_api.getRelatedEntities('inputFiles', process, 'files'))
        for input_file in input_files:
            uuid = input_file['uuid']['uuid']
            process_info.input_files[uuid] = input_file
            derived_by_processes.extend(
                self.ingest_api.getRelatedEntities('derivedByProcesses', input_file, 'processes'))

        derived_biomaterials = list(self.ingest_api.getRelatedEntities('derivedBiomaterials', process, 'biomaterials'))
        derived_files = list(self.ingest_api.getRelatedEntities('derivedFiles', process, 'files'))

        # since wrapper processes are not included in process bundle,
        #  links to it must be applied to its chained processes
        processes_to_link = chained_processes if is_wrapper else [process]
        for process_to_link in processes_to_link:
            process_uuid = process_to_link['uuid']['uuid']

            protocols = list(self.ingest_api.getRelatedEntities('protocols', process_to_link, 'protocols'))
            for protocol in protocols:
                uuid = protocol['uuid']['uuid']
                process_info.protocols[uuid] = protocol

            if input_biomaterials:
                if derived_files:
                    process_info.links.append({
                        'process': process_uuid,
                        'inputs': [input_biomaterial['uuid']['uuid'] for input_biomaterial in input_biomaterials],
                        'input_type': 'biomaterial',
                        'outputs': [derived_file['uuid']['uuid'] for derived_file in derived_files],
                        'output_type': 'file',
                        'protocols': [
                            {
                                'protocol_type': self.get_concrete_entity_type(protocol),
                                'protocol_id': protocol['uuid']['uuid']
                            } for protocol in protocols
                        ]
                    })

                if derived_biomaterials:
                    process_info.links.append({
                        'process': process_uuid,
                        'inputs': [input_biomaterial['uuid']['uuid'] for input_biomaterial in input_biomaterials],
                        'input_type': 'biomaterial',
                        'outputs': [derived_biomaterial['uuid']['uuid'] for derived_biomaterial in derived_biomaterials],
                        'output_type': 'biomaterial',
                        'protocols': [
                            {
                                'protocol_type': self.get_concrete_entity_type(protocol),
                                'protocol_id': protocol['uuid']['uuid']
                            } for protocol in protocols
                        ]
                    })

            if input_files and derived_files:
                process_info.links.append({
                    'process': process_uuid,
                    'inputs': [input_file['uuid']['uuid'] for input_file in input_files],
                    'input_type': 'file',
                    'outputs': [derived_file['uuid']['uuid'] for derived_file in derived_files],
                    'output_type': 'file',
                    'protocols': [
                        {
                            'protocol_type': self.get_concrete_entity_type(protocol),
                            'protocol_id': protocol['uuid']['uuid']
                        } for protocol in protocols
                    ]
                })

        for derived_by_process in derived_by_processes:
            self.recurse_process(derived_by_process, process_info)

    def get_input_bundle(self, process):
        bundle_manifests = list(self.ingest_api.getRelatedEntities('inputBundleManifests', process, 'bundleManifests'))

        if len(bundle_manifests) > 0:
            return bundle_manifests[0]

        return None

    def prepare_metadata_files(self, metadata_info,  is_indexed=True) -> 'dict':
        metadata_files_by_type = dict()

        for entity_type in ['biomaterial', 'file', 'project', 'protocol', 'process']:
            metadata_files_by_type[entity_type] = list()
            specific_types_counter = dict()
            for (metadata_uuid, doc) in metadata_info[entity_type].items():
                specific_entity_type = self.get_concrete_entity_type(doc)

                specific_types_counter[specific_entity_type] = 0 if specific_entity_type not in specific_types_counter else specific_types_counter[specific_entity_type] + 1

                file_name = '{0}_{1}.json'.format(specific_entity_type, specific_types_counter[specific_entity_type])
                upload_filename = '{0}_{1}.json'.format(specific_entity_type, metadata_uuid)

                prepared_doc = {
                    'content': self.bundle_metadata(doc, metadata_uuid),
                    'content_type': '"metadata/{0}"'.format(entity_type),
                    'indexed': is_indexed,
                    'dss_filename': file_name,
                    'dss_uuid': metadata_uuid,
                    'upload_filename': upload_filename
                }

                metadata_files_by_type[entity_type].append(prepared_doc)

        return metadata_files_by_type

    def bundle_metadata(self, metadata_doc, uuid):
        provenance_core = dict()
        provenance_core['document_id'] = uuid
        provenance_core['submission_date'] = metadata_doc['submissionDate']
        provenance_core['update_date'] = metadata_doc['updateDate']

        bundle_doc = metadata_doc['content']
        bundle_doc['provenance'] = provenance_core

        return bundle_doc

    def bundle_links(self, links):
        # TODO do not hard code schema, query latest from schema endpoint
        return {
            'describedBy': urljoin(self.schema_url, '/system/1.1.1/links'),
            'schema_type': 'link_bundle',
            'schema_version': '1.1.1',
            'links': links
        }

    def upload_metadata_files(self, submission_uuid, metadata_files_info):
        try:
            for metadata_type in ['project', 'biomaterial', 'process', 'protocol', 'file', 'links']:
                for metadata_doc in metadata_files_info[metadata_type]:
                    bundle_file = metadata_doc
                    filename = bundle_file['upload_filename']
                    content = bundle_file['content']
                    content_type = bundle_file['content_type']

                    uploaded_file = self.upload_file(submission_uuid, filename, content, content_type)
                    bundle_file['upload_file_url'] = uploaded_file.url
        except Exception as e:
            message = "An error occurred on uploading bundle files: " + str(e)
            raise BundleFileUploadError(message)

    def put_bundle_in_dss(self, bundle_uuid, created_files):
        try:
            created_bundle = self.dss_api.put_bundle(bundle_uuid, created_files)
        except Exception as e:
            message = 'An error occurred while putting bundle in DSS: ' + str(e)
            raise BundleDSSError(message)

        return created_bundle

    def put_files_in_dss(self, bundle_uuid, files_to_put):
        created_files = []

        for bundle_file in files_to_put:
            version = ''

            try:
                created_file = self.dss_api.put_file(bundle_uuid, bundle_file)
                version = created_file['version']
            except Exception as e:
                raise FileDSSError('An error occurred while putting file in DSS' + str(e))

            file_param = {
                "indexed": bundle_file["indexed"],
                "name": bundle_file["submittedName"],
                "uuid": bundle_file["dss_uuid"],
                "content-type": bundle_file["content-type"],
                "version": version
            }

            created_files.append(file_param)

        return created_files

    def get_metadata_files(self, metadata_files_info):
        metadata_files = []

        for entity_type in ['biomaterial', 'file', 'project', 'protocol', 'process', 'links']:
            for metadata_file in metadata_files_info[entity_type]:
                metadata_files.append({
                    'name': metadata_file['upload_filename'],
                    'submittedName': metadata_file['dss_filename'],
                    'url': metadata_file['upload_file_url'],
                    'dss_uuid': metadata_file['dss_uuid'],
                    'indexed': metadata_file['indexed'],
                    'content-type': metadata_file['content_type']
                })

        return metadata_files

    def get_data_files(self, uuid_file_dict):
        data_files = []
        #  TODO: need to keep track of UUIDs used so that retries work when the DSS returns a 500
        for file_uuid, data_file in uuid_file_dict.items():
            filename = data_file['fileName']
            cloud_url = data_file['cloudUrl']
            data_file_uuid = data_file['dataFileUuid']

            data_files.append({
                'name': filename,
                'submittedName': filename,
                'url': cloud_url,
                'dss_uuid': data_file_uuid,
                'indexed': False,
                'content-type': 'data'
            })

        return data_files

    def create_bundle_manifest(self, submission_uuid, files_by_type):
        bundle_manifest = ingestapi.BundleManifest()
        bundle_manifest.envelopeUuid = submission_uuid

        bundle_manifest.fileProjectMap = dict()
        for metadata_file in files_by_type['project']:
            bundle_manifest.fileProjectMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        bundle_manifest.fileBiomaterialMap = dict()
        for metadata_file in files_by_type['biomaterial']:
            bundle_manifest.fileBiomaterialMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        bundle_manifest.fileProcessMap = dict()
        for metadata_file in files_by_type['process']:
            bundle_manifest.fileProcessMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        bundle_manifest.fileProtocolMap = dict()
        for metadata_file in files_by_type['protocol']:
            bundle_manifest.fileProtocolMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        bundle_manifest.fileFilesMap = dict()
        for metadata_file in files_by_type['file']:
            bundle_manifest.fileFilesMap[metadata_file['dss_uuid']] = [metadata_file['dss_uuid']]

        return bundle_manifest

    def get_concrete_entity_type(self, schema_uri):
        return schema_uri["content"]["describedBy"].rsplit('/', 1)[-1]

    def upload_file(self, submission_uuid, filename, content, content_type):
        self.logger.info("writing to staging area..." + filename)
        file_description = self.staging_api.stageFile(submission_uuid, filename, content, content_type)
        self.logger.info("File staged at " + file_description.url)
        return file_description

    def dump_to_file(self, content, filename, output_dir=None):
        if output_dir:
            self.outputDir = output_dir

        if self.outputDir:
            dir = os.path.abspath(self.outputDir)
            if not os.path.exists(dir):
                os.makedirs(dir)
            tmpFile = open(dir + "/" + filename + ".json", "w")
            tmpFile.write(content)
            tmpFile.close()

class File:
    def __init__(self):
        self.name = ""
        self.content_type = ""
        self.size = ""
        self.id = ""
        self.checksums = {}


class ProcessInfo:
    def __init__(self):
        self.project = {}

        # uuid => object mapping
        self.input_biomaterials = {}
        self.derived_by_processes = {}
        self.input_files = {}
        self.derived_files = {}
        self.protocols = {}

        self.links = []

        self.input_bundle = None


# Module Exceptions


class Error(Exception):
    """Base-class for all exceptions raised by this module."""


class MultipleProjectsError(Error):
    """A process should only have one project linked."""


class InvalidBundleError(Error):
    """There was a failure in bundle validation."""

class BundleFileUploadError(Error):
    """There was a failure in bundle file upload."""


class BundleDSSError(Error):
    """There was a failure in bundle creation in DSS."""


class FileDSSError(Error):
    """There was a failure in file creation in DSS."""


class NoUploadAreaFoundError(Error):
    """Export couldn't be as no upload area found"""


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = OptionParser()
    parser.add_option("-e", "--submissionEnvelopeUuid",
                      help="Submission envelope UUID for which to generate the bundle")
    parser.add_option("-p", "--processUuid",
                      help="Process UUID")
    parser.add_option("-D", "--dry", help="do a dry run without submitting to ingest", action="store_true",
                      default=False)
    parser.add_option("-o", "--output", dest="output",
                      help="output directory where to dump json files submitted to ingest", metavar="FILE",
                      default=None)
    parser.add_option("-i", "--ingest", help="the URL to the ingest API")
    parser.add_option("-s", "--staging", help="the URL to the staging API")
    parser.add_option("-d", "--dss", help="the URL to the datastore service")
    parser.add_option("-l", "--log", help="the logging level", default='INFO')

    (options, args) = parser.parse_args()

    if not options.submissionEnvelopeUuid:
        print ("You must supply a Submission Envelope UUID")
        exit(2)

    if not options.processUuid:
        print ("You must supply a process UUID.")
        exit(2)

    if not options.ingest:
        print ("You must the url of Ingest API.")
        exit(2)

    exporter = IngestExporter(options)
    exporter.export_bundle(options.submissionsEnvelopeUuid, options.processUuid)
