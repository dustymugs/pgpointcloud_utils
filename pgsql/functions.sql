SET search_path = pointcloud;

CREATE OR REPLACE FUNCTION _PC_Transform(pt pcpoint, pcid integer, mapping json, txid bigint DEFAULT txid_current())
RETURNS pcpoint
AS $$
import simplejson as json
from pgpointcloud_utils import PcFormat, PcPoint

global SD

def check_cache_txid():
    # contents of cache are expired every transaction
    last_txid = SD.get('last_txid', None)
    if last_txid is None or last_txid != txid:
        SD['last_txid'] = txid
        SD['formats'] = {}

def add_to_cache(prefix, key, value):

    SD[prefix][key] = value

def get_from_cache(prefix, key):

    return SD[prefix].get(key, None)

def get_pgpointcloud_format(pcid):

    pcformat = get_from_cache('formats', pcid)
    if pcformat is not None:
        return pcformat

    resultset = plpy.execute(
        "SELECT pc.srid, pc.schema, srs.proj4text FROM pointcloud_formats pc JOIN spatial_ref_sys srs ON pc.srid = srs.srid WHERE pc.pcid = {pcid}".format(
            pcid=pcid
        ),
        1
    )

    if len(resultset) < 1:
        plpy.error("No record found in pointcloud_formats for PCID: {pcid}".format(
            pcid=pcid
        ))

    pcformat = PcFormat.import_format(
        pcid=pcid,
        srid=resultset[0]['srid'],
        schema=resultset[0]['schema']
    )
    pcformat.proj4text = resultset[0]['proj4text']
    add_to_cache(formats, pcid, pcformat)

    return pcformat

check_cache_txid()

# extract PCID from PcPoint
from_pcid = PcPoint.extract_pcid_from_hex(pt)

# if From PCID == To PCID, return PcPoint
if from_pcid == pcid:
    return pt

# get format of From PCID
from_format = get_pgpointcloud_format(from_pcid)

# get format of To PCID
to_format = get_pgpointcloud_format(pcid)

# load mapping
raw_mapping = json.loads(mapping)

# process keys
_mapping = {}
for k, v in raw_mapping.iteritems():
    try:
        k = int(k)
    except:
        _k = k

    _mapping[_k] = v

# deserialize pt
from_pcpoint = PcPoint.from_hex(from_format, pt)

# process and return
to_pcpoint = from_pcpoint.transform(to_format, _mapping)

return to_pcpoint.as_hex()

$$ LANGUAGE plpython2u STABLE;

CREATE OR REPLACE FUNCTION PC_Transform(pt pcpoint, pcid integer, mapping json)
RETURNS pcpoint
AS $$
	SELECT _PC_Transform($1, $2, $3)
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION PC_Transform(pa pcpatch, pcid integer, mapping json)
RETURNS pcpatch
AS $$
DECLARE
	pt pcpoint;
	pts pcpoint[] DEFAULT ARRAY[]::pcpoint[];
BEGIN
	FOR pt IN SELECT PC_Explode(pa) LOOP
	  pts := pts || _PC_Transform(pt, pcid, mapping);
	END LOOP;

	RETURN PC_Patch(pts);
END;
$$ LANGUAGE plpgsql STABLE;
