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
        r'aiohttp==3.6.2',
        r'aiomysql==0.0.20',
        r'aioredis==1.3.1',
        r'async-timeout==3.0.1',
        r'cachetools==4.1.1',
        r'crontab==0.22.8',
        r'cryptography==2.9.2',
        r'hiredis==1.0.1',
        r'Jinja2==2.11.2',
        r'tornado-jinja2==0.2.4',
        r'loguru==0.5.1',
        r'motor==2.1.0',
        r'numpy==1.19.0',
        r'ntplib==0.3.4',
        r'objgraph==3.4.1',
        r'Pillow==7.2.0',
        r'psutil==5.7.0',
        r'PyJWT==1.7.1',
        r'pytest==5.4.3',
        r'pytest-asyncio==0.14.0',
        r'python-stdnum==1.13',
        r'pyzmq==19.0.1',
        r'qrcode==6.1',
        r'mq-http-sdk==1.0.1',
        r'Sphinx==3.1.1',
        r'SQLAlchemy==1.3.18',
        r'tornado==6.0.4',
        r'terminal-table==2.0.1',
        r'ujson==3.0.0',
        r'WTForms==2.3.1',
        r'wtforms-tornado==0.0.2',
        r'xlwt==1.3.0',
        r'xmltodict==0.12.0',
        r'uvloop==0.14.0 ; sys_platform!="win32"',
    ],
    classifiers=[
        r'Programming Language :: Python :: 3.7',
        r'License :: OSI Approved :: Apache Software License',
        r'Operating System :: POSIX :: Linux',
    ],
)
