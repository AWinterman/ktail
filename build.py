from pybuilder.core import use_plugin, init, Author

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
use_plugin("python.coverage")
use_plugin("python.distutils")
use_plugin('filter_resources')


name = "ktail"

authors = [Author('Marco Hoyer', 'marco_hoyer@gmx.de')]
description = "ktail - A tail for AWS Kinesis streams decoding JSON messages."
license = 'APACHE LICENSE, VERSION 2.0'
summary = 'AWS Kinesis tail'
url = 'https://github.com/marco-hoyer/ktail'
version = '0.1.2'

default_task = ['clean', 'analyze', 'package']

@init
def set_properties(project):
    project.depends_on('boto3')
    project.depends_on('click')
    project.set_property('coverage_break_build', False)
    project.set_property('install_dependencies_upgrade', True)
    project.get_property('filter_resources_glob').extend(['**/kinesis_tail/__init__.py'])

    project.set_property('distutils_classifiers', [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: System :: Systems Administration'
    ])
