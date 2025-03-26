from setuptools import setup, find_packages

setup(
    name='emissions_mtcc',
    version='0.0.1',
    author='Gabriel Fuentes',
    author_email='Gabriel.Fuentes@nhh.no',
    description='Wrappers for MTCC estimations',
    packages=find_packages(),  # Automatically find all packages
    include_package_data=True,  # Ensures non-code files are included
    package_data={
        # Include all CSV files in `ghg4_tables` and `polygons`
        '': ['ghg4_tables/*.csv', 'polygons/*.csv','vessel_types_adjust/*.csv']
    },
    install_requires=["pyproj", "pykalman","scikit-learn"],
)
