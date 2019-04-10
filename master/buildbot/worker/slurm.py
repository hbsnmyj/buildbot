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

class SLURMLatentWorker(AbstractLatentWorker):
    def __init__(self, worker_name, password, host, username, buildbot_path, working_directory, batch_file, job_time, partition):
        self.host = host
        self.username = username
        self.worker_name = worker_name
        self.instance = None
        self.buildbot_path = buildbot_path
        self.working_directory = working_directory
        self.batch_file = batch_file
        self.job_time = job_time
        self.partition = partition
        self._poll_resolution = 5
        self.host_string = "%s@%s" %(self.host, self.username)

        super().__init__(username, password, **kwargs)

    def start_instance(self, build):
        if self.instance is not None:
            raise ValueError('instance active')
        return threads.deferToThread(self._start_instance)

    def _start_instance(self):
        cd_command = "echo cd %s" % self.working_directory;
        sbatch_command = "echo sbatch %s %s %s -p %s --time %s --job-name=%s" % (self.batch_file, \
                self.worker_name, self.partition, self._get_sec(self.job_time), self.job_time, \
                "worker")
        command = "%s && %s" % (cd_command, sbatch_command)
        result = subprocess.run(['ssh',self.host_string, command], capture_output=True)
        match = re.search(r'Submitted batch job (\d+)', result.output)
        if match:
            self.instance = {"jobid":match.group(1), "state":"PENDING"}
        else:
            raise LatentWorkerFailedToSubstantiate("Submitted job failed:" + result.output)

        instance_id, start_time = self._wait_for_instance()
        if None not in [instance_id, start_time]:
            return [intsance_id, start_time]
        else:
            self.failed_to_start(self.instance["jobid"], self.instance["state"])

    def _wait_for_instance(self):
        log.msg("%s %s wating for job %s to start.", self.__class__.__name__, self.worker_name, self.instance)
        duration = 0
        interval = self._poll_resolution

        while self.instance["state"] == "PENDING":
            time.sleep(_poll_resolution)
            duration += _poll_resolution
            log.msg("%s %s wating for job %s to start, time elapsed: %s seconds.", \
                    self.__class__.__name__, self.worker_name, self.instance, duration)
            if _poll_resolution < 300:
                _poll_resolution *= 2
            command = "scontrol show jobid=%s" % self.instance['jobid']
            result = subprocess.run(['ssh',self.host_string, command], capture_output=True)
            match = re.search(r'JobState=([A-Za-z_]*) ', result.output)
            if not match:
                raise LatentWorkerFailedToSubstantiate("Submitted job failed:" + result.output)
            self.instance["state"] = match.group(1)

        if self.instance["state"] = "RUNNING":
            minutes = duration // 60
            seconds = duration % 60
            start_time = '%02d:%02d:%02d' % (minutes // 60, minutes % 60, seconds)

            return self.instance["jobid"], start_time
        else
            self.failed_to_start(self.instance["id"], self.instance.instance['state'])

    def stop_instance(self, fast=False):
        return threads.deferToThread(self._stop_instance, fast)

    def _stop_instance(self, fast):
        pass

    def _get_sec(self, time_str):
        h, m, s = time_str.split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)

if __name__ == '__main__':
    worker = SLURMLatentWorker("", "", "worker_name","buildbot_path", "working_directory", "batch_file", "02:00:00", "storage")
    worker._start_instance()
