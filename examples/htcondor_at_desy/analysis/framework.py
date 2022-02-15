# coding: utf-8

"""
Law example tasks to demonstrate HTCondor workflows at DESY.

In this file, some really basic tasks are defined that can be inherited by
other tasks to receive the same features. This is usually called "framework"
and only needs to be defined once per user / group / etc.
"""


import os
import math

import luigi
import law


# the htcondor workflow implementation is part of a law contrib package
# so we need to explicitly load it
law.contrib.load("htcondor")


class Task(law.Task):
    """
    Base task that we use to force a version parameter on all inheriting tasks, and that provides
    some convenience methods to create local file and directory targets at the default data path.
    """

    version = luigi.Parameter()

    def store_parts(self):
        return (self.__class__.__name__, self.version)

    def local_path(self, *path):
        # ANALYSIS_DATA_PATH is defined in setup.sh
        parts = (os.getenv("ANALYSIS_DATA_PATH"),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is HTCondor. Law does not aim
    to "magically" adapt to all possible HTCondor setups which would certainly end in a mess.
    Therefore we have to configure the base HTCondor workflow in law.contrib.htcondor to work with
    the DESY HTCondor environment. In most cases, like in this example, only a minimal amount of
    configuration is required.
    """

    # also see below
    max_runtime = law.DurationParameter(default=3.0, unit="h", significant=False,
        description="maximum runtime, default unit is hours, default: 3")

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path())

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return law.util.rel_path(__file__, "bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):
        # render_variables are rendered into all files sent with a job
        config.render_variables["analysis_path"] = os.getenv("ANALYSIS_PATH")
        # force to run on CC7, http://batchdocs.web.cern.ch/batchdocs/local/submit.html#os-choice
        config.custom_content.append(("requirements", "(OpSysAndVer =?= \"CentOS7\")"))
        
        # do not use the +MaxRuntime for BIRD, as a standard job could be filled in any available slot (run on opportunistic quota),
        # see https://confluence.desy.de/display/IS/Submitting+Jobs
        # > A default job gets 1 core, 2 GB of ram and 3 h run time (job gets killed after 3 hours), here are the submit file entries to alter these defaults:
        # >  'Request_Cpus = <num>' # number of requested cpu-cores
        # >  'Request_Memory = <quantity>' # memory in MiB e.g 512, 1GB etc ...
        # >  '+RequestRuntime = <seconds>' # requested run time in seconds
        '''
        # maximum runtime
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))
        '''
        
        # copy the entire environment
        config.custom_content.append(("getenv", "true"))
        
        # at DESY Bird, log refers to HTCondor itself, output is given by the executable;
        # law combines stdout and stderr into stdall.txt, therefore the only missing information is log
        #config.custom_content.append(("log", os.getenv("ANALYSIS_PATH") + "/logs"))
        
        # put job on hold depending on exit status / signal / code
        config.custom_content.append(("on_exit_hold", "(ExitBySignal == True) || (ExitStatus != 0) || (ExitCode != 0)"))
        return config
