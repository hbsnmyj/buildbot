"""
A latent worker that uses SLURM to instantiate the workers on demand.
Tested with Python boto 1.5c
"""

from twisted.internet import defer
from twisted.internet import threads
from twisted.python import log

from buildbot import config
from buildbot.interfaces import LatentWorkerFailedToSubstantiate
from buildbot.worker import AbstractLatentWorker
from buildbot.worker_transition import reportDeprecatedWorkerNameUsage

import re
import subprocess
import time

class SLURMLatentWorker(AbstractLatentWorker):
    def __init__(self, name, password, host, username, buildbot_path, working_directory, batch_file, job_time, partition, **kwargs):
        self.host = host
        self.username = username
        self.worker_name = name
        self.instance = None
        self.buildbot_path = buildbot_path
        self.working_directory = working_directory
        self.batch_file = buildbot_path + "/" + batch_file
        self.job_time = job_time
        self.partition = partition
        self._poll_resolution = 5
        self.host_string = "%s@%s" %(self.username, self.host)

        super().__init__(name, password, **kwargs)

    def start_instance(self, build):
        if self.instance is not None:
            raise ValueError('instance active')
        return threads.deferToThread(self._start_instance)

    def _start_instance(self):
        cd_command = "cd %s" % self.working_directory;
        sbatch_command = "sbatch %s %s %s %s -p %s --time %s --job-name=%s" % (self.batch_file,
                self.buildbot_path,
                self.worker_name, self._get_sec(self.job_time), self.partition, 
                self.job_time, "worker")
        command = "%s && %s" % (cd_command, sbatch_command)

        result = subprocess.run(['ssh',self.host_string, command], capture_output=True)
        match = re.search(r'Submitted batch job (\d+)', result.stdout.decode("utf-8"))
        if match:
            self.instance = {"jobid":match.group(1), "state":"PENDING"}
            log.msg("%s %s submitting job %s, command:\n%s", self.__class__.__name__, self.worker_name, self.instance["jobid"],command)
        else:
            raise LatentWorkerFailedToSubstantiate("Submitted job failed:" +
                    "command is " + command,
                    result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8"))

        instance_id, start_time = self._wait_for_instance()
        if None not in [instance_id, start_time]:
            return [instance_id, start_time]
        else:
            self.failed_to_start(self.instance["jobid"], self.instance["state"])

    def _wait_for_instance(self):
        log.msg("%s %s wating for job %s to start.", self.__class__.__name__, self.worker_name, self.instance)
        duration = 0
        interval = self._poll_resolution

        while self.instance["state"] == "PENDING":
            time.sleep(self._poll_resolution)
            duration += self._poll_resolution
            log.msg("%s %s wating for job %s to start, time elapsed: %s seconds.", \
                    self.__class__.__name__, self.worker_name, self.instance, duration)
            if self._poll_resolution < 300:
                self._poll_resolution *= 2
            command = "scontrol show jobid=%s" % self.instance['jobid']
            result = subprocess.run(['ssh',self.host_string, command], capture_output=True)
            match = re.search(r'JobState=([A-Za-z_]*) ', result.stdout.decode("utf-8"))
            if not match:
                raise LatentWorkerFailedToSubstantiate("Submitted job failed:" +
                        result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8"))
            self.instance["state"] = match.group(1)
            match2 = re.search(r'^\s*NodeList=(\S+)', result.stdout.decode("utf-8"), re.MULTILINE)
            log.msg("", result.stdout.decode("utf-8"))
            if match2:
                self.instance["nodelist"]=match2.group(1)
            else:
                self.instance["nodelist"]=None

        if self.instance["state"] == "RUNNING":
            minutes = duration // 60
            seconds = duration % 60
            start_time = '%02d:%02d:%02d' % (minutes // 60, minutes % 60, seconds)

            return self.instance["jobid"], start_time
        else:
            self.failed_to_start(self.instance["jobid"], self.instance['state'])

    def stop_instance(self, fast=False):
        return threads.deferToThread(self._stop_instance, fast)

    def _stop_instance(self, fast):
        command = "/bin/bash %s/worker-slurm-stop.sh %s %s %s" % \
                (self.buildbot_path, self.buildbot_path, self.worker_name, self.instance["nodelist"])
        log.msg("%s %s wating for job %s to stop.", self.__class__.__name__, self.worker_name, self.instance)
        result = subprocess.run(['ssh',self.host_string, command], capture_output=True)
        log.msg("Command: %s", command)
        log.msg("Output: %s", result.stdout.decode("utf-8"))
        command2 = "scancel %s" % self.instance["jobid"]
        result2 = subprocess.run(['ssh',self.host_string, command2], capture_output=True)
        log.msg("Command: %s", command2)
        log.msg("Output: %s", result2.stdout.decode("utf-8"))

    def _get_sec(self, time_str):
        h, m, s = time_str.split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)

if __name__ == '__main__':
    worker = SLURMLatentWorker("", "", "worker_name","buildbot_path", "working_directory", "batch_file", "02:00:00", "storage")
    worker._start_instance()
