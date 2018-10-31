import os

from setuptools import setup, find_packages

module_dir = os.path.dirname(os.path.abspath(__file__))
setup(name='workspaces',
      version='0.1',
      description='explore structured data in workspace enviornments',
      author='Maxwell Dylla',
      license='MIT',
      packages=find_packages(),
      install_requires=['numpy', 'pandas', 'sklearn', 'plotly',
                        'matminer', 'pymatgen', 'pymongo'],
      long_description=open('readme.md').read())
