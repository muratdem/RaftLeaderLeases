from setuptools import setup, Extension

module = Extension('linearize', sources=['linearize.c'])

setup(name='Linearize',
      version='1.0',
      description='Linearizability-checking acceleration module',
      ext_modules=[module])
