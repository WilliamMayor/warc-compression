from setuptools import setup, find_packages
from pip.req import parse_requirements

install_reqs = parse_requirements('requirements.txt')
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='experiments',
    packages=find_packages(),
    install_requires=reqs)