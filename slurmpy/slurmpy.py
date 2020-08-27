r"""
# send in job name and kwargs for slurm params:
>>> s = Slurm("job-name", {"account": "ucgd-kp", "partition": "ucgd-kp"})
>>> print(str(s))
#!/bin/bash
<BLANKLINE>
#SBATCH -e logs/job-name.%J.err
#SBATCH -o logs/job-name.%J.out
#SBATCH -J job-name
#SBATCH --no-requeue
<BLANKLINE>
#SBATCH -J -Q
#SBATCH --no-requeue
#SBATCH --partition=hnm
#SBATCH --time=84:00:00
<BLANKLINE>
set -eo pipefail -o nounset
<BLANKLINE>
hostname
date
touch job-name.started
<BLANKLINE>
__script__
touch job-name.finished
<BLANKLINE>
>>> job_id = s.run("rm -f aaa; sleep 10; echo 213 > aaa", name_addition="")
>>> job = s.run("cat aaa; rm aaa", name_addition="", depends_on=[job_id])
>>> s.query(job, "JobState")
'PENDING'
"""

from __future__ import print_function

import atexit
import datetime
import hashlib
import os
import subprocess
import sys
import tempfile
import time

TMPL = """\
#!/bin/bash

#SBATCH -e logs/{name}.%J.err
#SBATCH -o logs/{name}.%J.out
#SBATCH -J {name}
#SBATCH --no-requeue

{header}

set -eo pipefail -o nounset

hostname
date
touch job-name.started
__script__
touch job-name.finished
"""


def tmp(suffix=".sh"):
    t = tempfile.mktemp(suffix=suffix)
    atexit.register(os.unlink, t)
    return t


class SlurmException(Exception):
    def init(self, *args, **kwargs):
        super(SlurmException, self).init(*args, **kwargs)
        pass


class Slurm(object):
    def __init__(self, name, slurm_kwargs=None, tmpl=None,
                 date_in_name=True, scripts_dir="slurm-scripts/"):
        if slurm_kwargs is None:
            slurm_kwargs = {}
        if tmpl is None:
            tmpl = TMPL

        header = []
        if 'time' not in slurm_kwargs.keys():
            slurm_kwargs['time'] = '84:00:00'
        for k, v in slurm_kwargs.items():
            if len(k) > 1:
                k = "--" + k + "="
            else:
                k = "-" + k + " "
            header.append("#SBATCH %s%s" % (k, v))

        self.header = "\n".join(header)
        self.name = "".join(x for x in name.replace(" ", "_") if x.isalnum() or x in ("-", "_"))
        self.tmpl = tmpl
        self.slurm_kwargs = slurm_kwargs
        if scripts_dir is not None:
            self.scripts_dir = os.path.abspath(scripts_dir)
        else:
            self.scripts_dir = None
        self.date_in_name = bool(date_in_name)

    def __str__(self):
        return self.tmpl.format(name=self.name, header=self.header)

    def _get_scriptname(self, name_addition=None):
        if self.scripts_dir is None:
            return tmp()
        else:
            if not os.path.exists(self.scripts_dir):
                os.makedirs(self.scripts_dir)

                script_name = self.name.strip("-")
                if name_addition:
                    script_name += name_addition.strip(" -")
                return "%s/%s.sh" % (self.scripts_dir, script_name)

    def run(self, command, name_addition=None, cmd_kwargs=None, local=False, depends_on=None, log_file=None,
            after=None):
        """
        command: a bash command that you want to run
        name_addition: if not specified, the shal of the command to run
               appended to job name. if it is "date", the yyyy-mm-dd
               date will be added to the job name.
        cmd_kwargs: diet of extra arguments to fill in command
             (so command itself can be a template).
        local: if True, run locally in the background (for testing). Returns the pid()
        depends_on: job ids that this depends on before it is run (uses 'afterok')
        after: job ids that this depends on them to STARTbefore it is run (uses 'after')
        """

        if name_addition is None:
            name_addition = hashlib.sha1(command.encode("utf-8")).hexdigest()

        if self.date_in_name:
            name_addition += "-" + datetime.datetime.fromtimestamp(time.time()).isoformat()
        name_addition = name_addition.strip(" -")
        script_name = self._get_scriptname(name_addition)

        print('script_name = ' + str(script_name)) #TODO: remove

        if cmd_kwargs is None:
            cmd_kwargs = {}

        args = []
        for k, v in cmd_kwargs.items():
            args.append("export %s=%s" % (k, v))
        args = "\n".join(args)

        tmpl = str(self).replace("__script__", args + "\n###\n" + command)
        if depends_on is None or (len(depends_on) == 1 and depends_on[0] is None):
            depends_on = []

        if after is None or (len(after) == 1 and after[0] is None):
            after = []

        if "logs/" in tmpl and not os.path.exists("logs/"):
            os.makedirs("logs")

        with open(script_name, "w") as sh:
            sh.write(tmpl)

        log_file = log_file if log_file else sys.stderr

        _cmd = 'bash' if local else 'sbatch'
        args = [_cmd]
        if not local:
            args.extend([("--dependency=afterok:%d" % int(d)) for d in depends_on])
            args.extend([("--dependency=after:%d" % int(d)) for d in after])
        args.append(sh.name)
        if not local:
            res = subprocess.check_output(args).strip()
            print(res, file=log_file)
            if not res.startswith(b"Submitted batch"):
                return None
            job_id = res.split()[-1]
            return job_id
        else:
            pid = subprocess.Popen(args, stdout=log_file, stderr=log_file)
            return "pid#" + str(pid.pid)

    @staticmethod
    def query(job_id, field=None, on_failure='exception'):
        try:
            ret = subprocess.check_output(["scontrol", "-d", "-o", "show", "job", str(job_id)],
                                          stderr=subprocess.STDOUT)
        except:
            if on_failure == 'warn':
                print("warning: scontrol query of job_id=%s failed" % str(job_id))
                return None
            elif on_failure == 'silent':
                return None
            else:
                raise SlurmException("Failed to query SLURM")

        try:
            ret_dict = {pair[0]: "=".join(pair[1:]) for pair in [_.split("=") for _ in ret.split()]}
        except:
            print('ret_dict failed for job_id=' + str(job_id) + ', ret=' + str(ret))
            raise SlurmException("Failed to create ret_dict")

        if field is not None:
            return ret_dict[field]
        return ret_dict

    @staticmethod
    def _still_running_pid(pid):
        try:
            with open('/dev/null', 'w') as devnull:
                subprocess.check_call(['ps', '-p', str(pid)], stdout=devnull, stderr=devnull)
        except subprocess.CalledProcessError:
            return False
        return True

    @staticmethod
    def _still_running_jobid(job_id):
        status = Slurm.query(job_id, field='JobState', on_failure='silent')
        if status in ('PENDING', 'RUNNING', 'SUSPENDED', 'CONFIGURING'):
            return True
        return False

    @staticmethod
    def still_running(job_id):
        if job_id is None:
            return False
        job_id = str(job_id)
        if job_id.startswith('pid#'):
            pid = job_id[4:]
            return Slurm._still_running_pid(pid)
        else:
            return Slurm._still_running_jobid(job_id)

    @staticmethod
    def kill(job_id):
        if job_id is None:
            return False
        job_id = str(job_id)
        if job_id.startswith('pid#'):
            pid = job_id[4:]
            os.system('kill -9 ' + pid)
            return True
        else:
            os.system('scancel ' + job_id)
            return True


if __name__ == "__main__":
    import doctest

    doctest.testmod()
