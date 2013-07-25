#coding: utf-8

import xlrd
import os
from boto.s3.connection import S3Connection
from boto.s3.key import Key

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from ckanext.harvest.harvesters import HarvesterBase

from pylons import config

import logging
log = logging.getLogger(__name__)

class SFAHarvester(HarvesterBase):
    '''
    The harvester for the SFA
    '''

    BUCKET_NAME = u'bar-opendata-ch'
    METADATA_FILE_NAME = u'OGD@Bund Metadaten BAR.xlsx'
    DEPARTMENT_BASE = u'ch.bar.'
    FILES_BASE_URL = 'http://bar-opendata-ch.s3.amazonaws.com'

    # Define the keys in the CKAN .ini file
    AWS_ACCESS_KEY = config.get('ckanext.sfa.access_key')
    AWS_SECRET_KEY = config.get('ckanext.sfa.secret_key')

    ORGANIZATION = {
        'de': u'Schweizerisches Bundesarchiv',
        'fr': u'Archives fédérales suisses',
        'it': u'Archivio federale svizzero',
        'en': u'Swiss Federal Archives',
    }

    config = {
        'user': u'harvest'
    }

    def _get_s3_bucket(self):
        '''
        Create an S3 connection to the department bucket
        '''
        conn = S3Connection(self.AWS_ACCESS_KEY, self.AWS_SECRET_KEY)
        bucket = conn.get_bucket(self.BUCKET_NAME)
        return bucket


    def _fetch_metadata_file(self):
        '''
        Fetching the Excel metadata file for the SFA from the S3 Bucket and save on disk
        '''
        try:
            metadata_file = Key(self._get_s3_bucket())
            metadata_file.key = self.METADATA_FILE_NAME
            metadata_file.get_contents_to_filename(self.METADATA_FILE_NAME)
            return True
        except Exception, e:
            log.exception(e)
            return False


    def _guess_format(self, file_name):
        '''
        Return the format for a given full filename
        '''
        _, file_extension = os.path.splitext(file_name.lower())
        return file_extension[1:]

    def _generate_resources_dict_array(self, dataset_id):
        '''

        '''
        try:
            resources = []
            prefix = self.DEPARTMENT_BASE + dataset_id + u'/'
            bucket_list = self._get_s3_bucket().list(prefix=prefix)
            for file in bucket_list:
                if file.key != prefix:
                    resources.append({
                        'url': self.FILES_BASE_URL + '/' + file.key,
                        'name': file.key.replace(prefix, u''),
                        'format': self._guess_format(file.key)
                        })
            return resources
        except Exception, e:
            log.exception(e)
            return []


    def info(self):
        return {
            'name': 'sfa',
            'title': 'SFA',
            'description': 'Harvests the SFA data',
            'form_config_interface': 'Text'
        }


    def gather_stage(self, harvest_job):
        log.debug('In SFAHarvester gather_stage')

        self._fetch_metadata_file()

        # Reading the metadata file
        metadata_workbook = xlrd.open_workbook(self.METADATA_FILE_NAME)
        worksheet = metadata_workbook.sheet_by_index(0)

        # Extract the row headers
        header_row = worksheet.row_values(6)

        ids = []
        for row_num in range(worksheet.nrows):

            # Data columns begin at row count 7 (8 in Excel)
            if row_num >= 7:
                row = dict(zip(header_row, worksheet.row_values(row_num)))
                
                # Construct the metadata dict for the dataset on CKAN
                metadata = {
                    'datasetID': row[u'id'],
                    'title': row[u'title'],
                    'notes': row[u'notes'],
                    'author': row[u'author'],
                    'maintainer': row[u'maintainer'],
                    'maintainer_email': row[u'maintainer_email'],
                    'license_id': row[u'licence'],
                    'translations': [],
                    'tags': row[u'tags'].split(u', '),
                    'groups': []
                }

                metadata['resources'] = self._generate_resources_dict_array(row[u'id'])
                log.debug(metadata['resources'])

                obj = HarvestObject(
                    guid = row[u'id'],
                    job = harvest_job,
                    content = json.dumps(metadata)
                )
                obj.save()
                log.debug('adding ' + row[u'id'] + ' to the queue')
                ids.append(obj.id)

                log.debug(dict(zip(header_row, worksheet.row_values(row_num))))

        return ids


    def fetch_stage(self, harvest_object):
        log.debug('In SFAHarvester fetch_stage')

        # Get the URL
        datasetID = json.loads(harvest_object.content)['datasetID']
        log.debug(harvest_object.content)

        # Get contents
        try:
            harvest_object.save()
            log.debug('successfully processed ' + datasetID)
            return True
        except Exception, e:
            log.exception(e)

    def import_stage(self, harvest_object):
        log.debug('In SFAHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        try:
            package_dict = json.loads(harvest_object.content)
            package_dict['id'] = harvest_object.guid
            package_dict['name'] = self._gen_new_name(package_dict[u'title'])

            user = model.User.get(self.config['user'])
            context = {
                'model': model,
                'session': Session,
                'user': self.config['user']
            }

            # Find or create the organization the dataset should get assigned to.
            try:
                data_dict = {
                    'permission': 'edit_group',
                    'id': self._gen_new_name(self.ORGANIZATION['de']),
                    'name': self._gen_new_name(self.ORGANIZATION['de']),
                    'title': self.ORGANIZATION['de']
                }
                package_dict['owner_org'] = get_action('organization_show')(context, data_dict)['id']
            except:
                organization = get_action('organization_create')(context, data_dict)
                package_dict['owner_org'] = organization['id']

            # Insert or update the package
            package = model.Package.get(package_dict['id'])
            pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            result = self._create_or_update_package(package_dict, harvest_object)

        except Exception, e:
            log.exception(e)

        return True
