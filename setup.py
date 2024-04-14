##########
# Import #
##############################################################################
from setuptools import setup, find_packages
import os

############
# Variable #
##############################################################################

__version__ = 0.2
__author__ = "Arnon Phongsiang" 
__email__ = "arnon.phongsiang@gmail.com"
__github__ = "https://github.com/MooArnon"
__medium__ = "https://medium.com/@oomarnon"
__linkedin__ = "https://www.linkedin.com/in/arnon-pongsiang-320796214/"

packages: list[str] = find_packages(
    exclude=[
        'tests', 
        'tests.*', 
        'experiment',
        'experiment.*',
        'run_local*',
        'run_local.*',
        'credential*'
    ]
)

requirements_path = os.path.join('requirements.txt')

################
# Description #
##############################################################################

with open("README.md", "r", encoding="utf-8") as fh:
    
    long_description = fh.read()

################
# Dependencies #
##############################################################################

with open("requirements.txt", "r") as file_obj:
    
    # reading the data from the file
    file_data = file_obj.read()

    # splitting the file data into lines
    dependencies = file_data.splitlines()
    
#########
# Setup #
##############################################################################

setup(
    name='space_time_pipeline',
    version=__version__,
    author=__author__,
    author_email=__email__,
    packages=packages,
    install_requires=dependencies,
    include_package_data=True,
    long_description = long_description,
)

##############################################################################
