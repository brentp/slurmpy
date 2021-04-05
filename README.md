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

The script to run() can also contain `$variables` which are filled with the cmd_kwarg dict.
E.g. `echo $name` could be filled with `cmd_kwargs={'name': 'sally'}`

A command can be tested (not sent to queue) by setting the `_cmd` are to `run` as e.g. "ls".
The default is `sbatch` which submits jobs to slurm.

Dependencies
============

Each time `slurmpy.Slurm().run()` is called, it returns the job-id of the submitted job. One or more
of these can be provided as arguments to a subsequent job:
```
s = Slurm()
s.run(..., depends_on=[job_id])
# or for multiple dependencies
s.run(..., depends_on=[job_id_1, job_id_2, job_id_3])

```
to indicate that this job should not run until the the job with `job_id` (or ids `job_id_1-3`) has finished successfully.

Additionally, SLURM allows you to define how a job depends on previous jobs e.g. should it run 
only on success or always etc. For this `slurmpy.Slurm().run()` provides an additional argument `depends_how`:

```
s = Slurmp()
s.run(..., depends_on=[job_id], depends_how='afterany')
```
currently supported are the following options with "afterok" as default: 
"after", "afterany", "afterburstbuffer", "aftercorr", "afternotok", "afterok", "expand"
More details can be found here: https://slurm.schedmd.com/sbatch.html in the `--dependency` section.

Install
=======

```Shell
pip install slurmpy --user
```
