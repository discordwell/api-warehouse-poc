from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
import sys
import setuptools

# Use pybind11's get_include()
import pybind11

class get_pybind_include(object):
    """Helper class to determine the pybind11 include path
    The purpose of this class is to postpone importing pybind11
    until it is actually installed, so that the ``get_include()``
    method can be invoked. """

    def __str__(self):
        return pybind11.get_include()

ext_modules = [
    Extension(
        'hbdb.native_ext',
        ['hbdb/native/module.cpp', 'hbdb/native/resolver.cpp', 'hbdb/native/backend.cpp'],
        include_dirs=[
            # Path to pybind11 headers
            get_pybind_include(),
            'hbdb/native'
        ],
        language='c++'
    ),
]

setup(
    name='hbdb_native',
    version='0.1',
    author='Antigravity',
    description='Native C++ extensions for HBDB',
    ext_modules=ext_modules,
    setup_requires=['pybind11>=2.5.0'],
    cmdclass={'build_ext': build_ext},
    zip_safe=False,
)
