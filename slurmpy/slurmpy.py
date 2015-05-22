r"""
# send in job name and kwargs for slurm params:
>>> s = Slurm("job-name", {"account": "ucgd-kp", "partition": "ucgd-kp"})
>>> print str(s)
#!/bin/bash
<BLANKLINE>
#SBATCH -e logs/job-name.%J.err
#SBATCH -o logs/job-name.%J.out
#SBATCH -J job-name
<BLANKLINE>
#SBATCH --account=ucgd-kp
#SBATCH --partition=ucgd-kp
<BLANKLINE>
set -eo pipefail -o nounset
<BLANKLINE>
{script}

#>>> s.run("do stuff")

"""


import os
import subprocess
import tempfile
import atexit
import hashlib

TMPL = """\
#!/bin/bash

#SBATCH -e logs/{name}.%J.err
#SBATCH -o logs/{name}.%J.out
#SBATCH -J {name}

{header}

set -eo pipefail -o nounset

{script}"""


def tmp(suffix=".sh"):
    t = tempfile.mktemp(suffix=suffix)
    atexit.register(os.unlink, t)
    return t


class Slurm(object):
    def __init__(self, name, slurm_kwargs=None, tmpl=None, scripts_dir="scripts/"):
        if slurm_kwargs is None:
            slurm_kwargs = {}
        if tmpl is None:
            tmpl = TMPL

        header = []
        for k, v in slurm_kwargs.items():
            if len(k) > 1:
                k = "--" + k + "="
            else:
                k = "-" + k + " "
            header.append("#SBATCH %s%s" % (k, v))
        self.header = "\n".join(header)
        self.name = name
        self.tmpl = tmpl
        self.slurm_kwargs = slurm_kwargs
        if scripts_dir is not None:
            self.scripts_dir = os.path.abspath(scripts_dir)
        else:
            self.scripts_dir = None

    def __str__(self):
        return self.tmpl.format(name=self.name, header=self.header,
                                script="{script}")

    def _tmpfile(self):
        if self.scripts_dir is None:
            return tmp()
        else:
            if not os.path.exists(self.scripts_dir):
                os.makedirs(self.scripts_dir)
            return "%s/%s.sh" % (self.scripts_dir, self.name)

    def run(self, command, name_addition=None, _cmd="sbatch", cmd_kwargs=None):
        """
        command: a bash command that you want to run
        name_addition: if not specified, the sha1 of the command to run
                       appended to job name
        _cmd: submit command (change to "bash" for testing).
        cmd_kwargs: dict of extra arguments to fill in command
                   (so command itself can be a template).
        """

        if name_addition is None:
            name_addition = hashlib.sha1(command).hexdigest()
        if cmd_kwargs is None:
            cmd_kwargs = {}

        n = self.name
        self.name += ("-" + name_addition).strip(" -")

        tmpl = str(self).format(script=command)

        with open(self._tmpfile(), "w") as sh:
            cmd_kwargs["script"] = command
            sh.write(tmpl.format(**cmd_kwargs))

        subprocess.check_call([_cmd, sh.name])
        self.name = n

if __name__ == "__main__":
    import doctest
    doctest.testmod()
