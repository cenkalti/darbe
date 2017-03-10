# coding=utf8
from setuptools import setup


setup(
    name='Darbe',
    version='1.1.5',
    author=u'Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='mysql rds migration database replication slave',
    url='https://github.com/cenk/darbe',
    py_modules=['darbe'],
    install_requires=[
        'boto3',
        'mysql-connector',
    ],
    description='RDS MySQL replication setup tool',
    entry_points={
        'console_scripts': [
            'darbe = darbe:main',
        ],
    },
)
