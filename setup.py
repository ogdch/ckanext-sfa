from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(
    name='ckanext-sfa',
    version=version,
    description="CKAN extension for the SFA for the OGD portal of Switzerland",
    long_description="""\
    """,
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Liip AG',
    author_email='ogd@liip.ch',
    url='http://www.liip.ch',
    license='GPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.sfa'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    entry_points=\
    """
    [ckan.plugins]
    sfa=ckanext.sfa.plugins:SfaHarvest
    sfa_harvester=ckanext.sfa.harvesters:SFAHarvester
    [paste.paster_command]
    harvester=ckanext.sfa.commands.harvester:Harvester
    """,
)
