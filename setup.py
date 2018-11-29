import os
from setuptools import setup


def read(*fname):
    with open(os.path.join(os.path.dirname(__file__), *fname)) as f:
        return f.read()


try:
    version = read('VERSION').strip()
except FileNotFoundError:
    version = '0'


setup(
    name='Darbe',
    version=version,
    author='Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='mysql rds migration database replication slave',
    url='https://github.com/cenk/darbe',
    py_modules=['darbe'],
    install_requires=[
        'boto3',
        'pymysql',
    ],
    description='RDS MySQL replication setup tool',
    entry_points={
        'console_scripts': [
            'darbe = darbe:main',
        ],
    },
)
