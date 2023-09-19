from setuptools import setup, find_packages

with open("requirements.txt", encoding="utf-8") as f:
    required = f.read().splitlines()

setup(
    name='DSCI 551 Database',
    version='0.1',
    description='Custom Database System',
    author='Flemming Wu',
    author_email='wuflemmi@usc.edu',
    packages=find_packages(),
    install_requires=required,
    python_requires=">=3.10"
)