from setuptools import setup

def get_version(path):
    """Get the version info from the mpld3 package without importing it"""
    import ast

    with open(path) as init_file:
        module = ast.parse(init_file.read())

    version = (ast.literal_eval(node.value) for node in ast.walk(module)
               if isinstance(node, ast.Assign)
               and node.targets[0].id == "__version__")
    try:
        return next(version)
    except StopIteration:
        raise ValueError("version could not be located")


setup(
    name="slurmpy",
    version=get_version("slurmpy/__init__.py"),
    author="Brent Pedersen",
    author_email="bpederse@gmail.com",
    description=("submit jobs to slurm with python"),
    license="MIT",
    keywords="cluster slurmpy",
    url="https://github.com/brentp/slurmpy",
    packages=['slurmpy'],
    long_description=open('README.md').read(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3'
    ],
)
