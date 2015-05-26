quick and dirty lib for submitting jobs to slurm via python2/python3.

```Python
from slurmpy import Slurm

s = Slurm("job-name", {"account": "my-account", "partition": "my-parition"})
s.run("""
do
lots
of
stuff
""")

```

The above will submit the job to `sbatch` automatically write the script to `scripts/`
and automatically write logs/{name}.err and logs/{name}.out. It will have today's
date in the log and script names.

It uses a template by default, but can be overriden with the kwargs dict as above.

The script to run() can also be a template which is filled with the cmd_kwarg dict.

A command can be tested (not sent to queue) by setting the `_cmd` are to `run` as e.g. "ls".
The default is `sbatch` which submits jobs to slurm.

Install
=======

```Shell
pip install slurmpy --user
```
