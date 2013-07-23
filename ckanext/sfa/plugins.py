import ckan
import ckan.plugins as p
from pylons import config

class SfaHarvest(p.SingletonPlugin):
    """
    Plugin containing the harvester for SFA
    """
