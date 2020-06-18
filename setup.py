# -*- coding: utf-8 -*-

import setuptools

from hagworm import __version__ as package_version


with open(r'README.md', r'r', encoding=r'utf8') as stream:
    long_description = stream.read()

setuptools.setup(
    name=r'hagworm',
    version=package_version,
    license=r'Apache License Version 2.0',
    platforms=[r'all'],
    author=r'Shaobo.Wang',
    author_email=r'wsb310@gmail.com',
    description=r'Network Development Suite',
    long_description=long_description,
    long_description_content_type=r'text/markdown',
    url=r'https://github.com/wsb310/hagworm',
    packages=setuptools.find_packages(),
    package_data={r'hagworm': [r'static/*.*']},
    python_requires=r'>= 3.7',
    install_requires=[
        r'aiohttp==3.5.4',
        r'aiomysql==0.0.20',
        r'aioredis==1.2.0',
        r'cacheout==0.11.1',
        r'crontab==0.22.6',
        r'cryptography==2.7.0',
        r'hiredis==1.0.0',
        r'Jinja2==2.10.1',
        r'tornado-jinja2==0.2.4',
        r'loguru==0.3.0',
        r'motor==2.0.0',
        r'numpy==1.18.2',
        r'ntplib==0.3.4',
        r'objgraph==3.4.1',
        r'Pillow==6.1.0',
        r'psutil==5.6.3',
        r'PyJWT==1.7.1',
        r'pytest==5.0.1',
        r'pytest-asyncio==0.10.0',
        r'python-stdnum==1.13',
        r'pyzmq==19.0.1',
        r'Sphinx==2.1.2',
        r'SQLAlchemy==1.3.5',
        r'tornado==6.0.3',
        r'terminal-table==1.0.8',
        r'uvloop==0.14.0',
        r'WTForms==2.2.1',
        r'wtforms-tornado==0.0.2',
        r'xlwt==1.3.0',
        r'xmltodict==0.12.0',
    ],
    classifiers=[
        r'Programming Language :: Python :: 3.7',
        r'License :: OSI Approved :: Apache Software License',
        r'Operating System :: POSIX :: Linux',
    ],
)
