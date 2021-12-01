from setuptools import setup

setup(
    name="glue_python_shell_sample_module",
    version="0.1",
    install_requires=[
        "pyarrow~=0.15.1",
        "s3fs~=0.4.0",
        "beautifulsoup4~=4.10.0",
        "bs4~=0.0.1",
        "lxml==4.6.4",
        "soupsieve==2.3.1",
        "urllib3==1.26.7"
    ]
) 