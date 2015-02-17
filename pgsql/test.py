import simplejson as json
from pgpointcloud_utils import PcFormat, PcPoint

global SD

def init_cache():

    if not isinstance(SD, dict):
        SD = {}

def add_to_cache(key, value):

    SD[key] = value

def get_from_cache(key):

    return SD.get(key, None)

def get_pgpointcloud_format(pcid):

    pcformat = get_from_cache(pcid)
    if pcformat is not None:
        return pcformat

    resultset = plpy.execute(
        "SELECT srid, schema FROM pointcloud_formats WHERE pcid = %d" % pcid,
        1
    )

    if len(resultset) < 1:
        plpy.error("No record found in pointcloud_formats for PCID: %d" % pcid)

    pcformat = PcFormat.import_format(
        pcid = pcid,
        srid=resultset[0]['srid'],
        xml=resultset[0]['schema']
    )
    add_to_cache(pcid, pcformat)

    return pcformat

init_cache()

# extract PCID from PcPoint
origin_pcid = PcPoint.extract_pcid_from_hex(pt)

# if origin PCID == destination PCID, return PcPoint
if origin_pcid == pcid:
    return pt

# get info of origin PCID
origin_format = get_pgpointcloud_format(origin_pcid)

# get info of destination PCID
destination_format = get_pgpointcloud_format(pcid)

# deserialize pcpoint
origin_pcpoint = PcPoint.from_hex(origin_format, pt)


# load mapping
try:
	_mapping = json.loads(mapping)
except json.JSONDecodeError:
    plpy.error("Cannot decode mapping JSON")

# check that mapping has all of destination schema positions/keywords
# simultaneously build conversion map

# run conversion
