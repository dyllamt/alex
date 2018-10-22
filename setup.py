import os

from setuptools import setup, find_packages

module_dir = os.path.dirname(os.path.abspath(__file__))
setup(name='alex',
      version='0.1',
      description='alloy exploration in materials databases',
      author='Maxwell Dylla',
      license='MIT',
      packages=find_packages(),
      install_requires=['numpy', 'pymongo', 'pandas', 'sklearn', 'plotly',
                        'matminer', 'pymatgen'],
      long_description=open('readme.md').read())
