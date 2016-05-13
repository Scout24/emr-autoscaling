from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
use_plugin("python.coverage")
use_plugin("python.distutils")
use_plugin("pypi:pybuilder_aws_plugin")

name = "emr-autoscaling"
default_task = ["install_build_dependencies", "package_lambda_code"]
version = "1.0"

@init
def set_properties(project):
    project.set_property("coverage_break_build", False)
    project.set_property("bucket_prefix", "emr/lambda_autoscaling/")
    project.build_depends_on("boto3")
    project.build_depends_on("mock")

@init(environments = "teamcity")
def set_properties_for_teamcity_builds(project):
    import os
    project.set_property("teamcity_output", True)
    project.version = "%s-%s" % (project.version, os.environ.get("BUILD_NUMBER", 0))
    project.default_task = ["install_build_dependencies", "package_lambda_code", "upload_zip_to_s3", "lambda_release"]
    project.set_property("install_dependencies_index_url", os.environ.get("PYPIPROXY_URL"))
    project.set_property("install_dependencies_use_mirrors", False)
    project.rpm_release = os.environ.get("RPM_RELEASE", 0)
