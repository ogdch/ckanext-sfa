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
from ckanext.harvest.harvesters.base import munge_tag

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
        u'de': u'Schweizerisches Bundesarchiv',
        u'fr': u'Archives fédérales suisses',
        u'it': u'Archivio federale svizzero',
        u'en': u'Swiss Federal Archives',
    }
    LANG_CODES = ['de', 'fr', 'it', 'en']

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


    def _get_row_dict_array(self, lang_index):
        '''
        '''
        try:
            metadata_workbook = xlrd.open_workbook(self.METADATA_FILE_NAME)
            worksheet = metadata_workbook.sheet_by_index(lang_index)

            # Extract the row headers
            header_row = worksheet.row_values(6)
            rows = []
            for row_num in range(worksheet.nrows):
                # Data columns begin at row count 7 (8 in Excel)
                if row_num >= 7:
                    rows.append(dict(zip(header_row, worksheet.row_values(row_num))))
            return rows

        except Exception, e:
            log.exception(e)
            return []


    def _generate_term_translations(self, lang_index):
        '''
        '''
        try:
            translations = []

            de_rows = self._get_row_dict_array(0)
            other_rows = self._get_row_dict_array(lang_index)

            log.debug(de_rows)
            log.debug(other_rows)

            keys = ['title', 'notes', 'author', 'maintainer', 'licence', 'groups']

            for row_idx in range(len(de_rows)):
                for key in keys:
                    translations.append({
                        'lang_code': self.LANG_CODES[lang_index],
                        'term': de_rows[row_idx][key],
                        'term_translation': other_rows[row_idx][key]
                        })

                de_tags = de_rows[row_idx]['tags'].split(u', ')
                other_tags = other_rows[row_idx]['tags'].split(u', ')

                if len(de_tags) == len(other_tags):
                    for tag_idx in range(len(de_tags)):
                        translations.append({
                            'lang_code': self.LANG_CODES[lang_index],
                            'term': munge_tag(de_tags[tag_idx]),
                            'term_translation': munge_tag(other_tags[tag_idx])
                            })

            for k,v in self.ORGANIZATION.items():
                if k != u'de':
                    translations.append({
                        'lang_code': k,
                        'term': self.ORGANIZATION[u'de'],
                        'term_translation': v
                        })

            return translations


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
        ids = []

        de_rows = self._get_row_dict_array(0)
        for row in de_rows:
            # Construct the metadata dict for the dataset on CKAN
            metadata = {
                'datasetID': row[u'id'],
                'title': row[u'title'],
                'url': row[u'url'],
                'notes': row[u'notes'],
                'author': row[u'author'],
                'maintainer': row[u'maintainer'],
                'maintainer_email': row[u'maintainer_email'],
                'license_id': row[u'licence'],
                'translations': [],
                'tags': row[u'tags'].split(u', '),
                'groups': [row[u'groups']]
            }

            metadata['resources'] = self._generate_resources_dict_array(row[u'id'])
            log.debug(metadata['resources'])

            # Adding term translations
            metadata['translations'].extend(self._generate_term_translations(1)) # fr
            metadata['translations'].extend(self._generate_term_translations(2)) # it
            metadata['translations'].extend(self._generate_term_translations(3)) # en

            log.debug(metadata['translations'])

            obj = HarvestObject(
                guid = row[u'id'],
                job = harvest_job,
                content = json.dumps(metadata)
            )
            obj.save()
            log.debug('adding ' + row[u'id'] + ' to the queue')
            ids.append(obj.id)

            log.debug(de_rows)

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

            # Find or create group the dataset should get assigned to
            for group_name in package_dict['groups']:
                try:
                    data_dict = {
                        'id': group_name,
                        'name': self._gen_new_name(group_name),
                        'title': group_name
                        }
                    group_id = get_action('group_show')(context, data_dict)['id']
                except:
                    group = get_action('group_create')(context, data_dict)
                    log.info('created the group ' + group['id'])

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

            # Add the translations to the term_translations table
            for translation in package_dict['translations']:
                action.update.term_translation_update(context, translation)
            Session.commit()

        except Exception, e:
            log.exception(e)

        return True
