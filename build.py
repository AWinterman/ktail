from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
use_plugin("python.coverage")
use_plugin("python.distutils")


name = "ktail"
default_task = "publish"


@init
def set_properties(project):
    project.depends_on('boto3')
    project.depends_on('click')
    project.set_property('coverage_break_build', False)
    project.set_property('install_dependencies_upgrade', True)
    project.get_property('filter_resources_glob').extend(['**/kinesis_tail/__init__.py', '**/scripts/ktail'])
