CREATE OR REPLACE FUNCTION PC_Transform(pt pcpoint, pcid integer, mapping json)
RETURNS pcpoint
AS $$
import simplejson as json

# get info of destination pcid
resultset = plpy.execute("SELECT srid, schema FROM pointcloud_formats WHERE pcid = %d" % pcid, 1)
if len(resultset) < 1:
	plpy.error("No record found in pointcloud_formats for PCID: %d" % pcid)
destination_format = resultset[0]

# get info of origin pcid from pt

# if origin pcid == destination pcid, no need to transform

# load mapping
try:
	_mapping = json.loads(mapping)
except json.JSONDecodeError:



$$ LANGUAGE plpython2u STABLE;
