ckanext-sfa
===========

Harvester for the Swiss Federal Archives (SFA)

## Installation

Use `pip` to install this plugin. This example installs it in `/home/www-data`

```bash
source /home/www-data/pyenv/bin/activate
pip install -e git+https://github.com/ogdch/ckanext-sfa.git#egg=ckanext-sfa --src /home/www-data
cd /home/www-data/ckanext-sfa
pip install -r pip-requirements.txt
python setup.py develop
```

Make sure to add `sfa` and `sfa_harvester` to `ckan.plugins` in your config file.

### For development
* install the `pre-commit.sh` script as a pre-commit hook in your local repositories:
** `ln -s ../../pre-commit.sh .git/hooks/pre-commit`

## Run harvester

```bash
source /home/www-data/pyenv/bin/activate
paster --plugin=ckanext-sfa sfa_harvester gather_consumer -c development.ini &
paster --plugin=ckanext-sfa sfa_harvester fetch_consumer -c development.ini &
paster --plugin=ckanext-sfa sfa_harvester run -c development.ini
```
