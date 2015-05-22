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
and automatically write logs/{name}.err and logs/{name}.out

It uses a template by default, but can be overriden with the kwargs dict as above.

The script to run() can also be a template which is filled with the cmd_kwarg dict.

Install
=======

```Shell
pip install slurmpy --user
```
