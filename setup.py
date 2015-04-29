from distutils.core import setup

setup(
    name='pgpointcloud_utils',
    version='0.1.5',
    author='Bborie Park',
    author_email='bboriepark@granular.ag',
    url='www.granular.ag',
    packages=['geojson2pgpc', 'ogr2pgpc', 'pgpointcloud_utils'],
    scripts=['scripts/geojson2pgpc', 'scripts/ogr2pgpc']
)
