# -*- coding: utf-8 -*-

import setuptools

with open(r'README.md', r'r') as stream:
    long_description = stream.read()

setuptools.setup(
    name=r'hagworm',
    version=r'0.0.80',
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
    python_requires=r'>= 3.6',
    install_requires=[
        r'aioftp==0.13.0',
        r'aiohttp==3.5.4',
        r'aiokafka==0.5.1',
        r'aiomysql==0.0.20',
        r'aioredis==1.2.0',
        r'aiotask_context==0.6.0',
        r'cacheout==0.11.1',
        r'crontab==0.22.5',
        r'cryptography==2.7.0',
        r'hiredis==1.0.0',
        r'Jinja2==2.10.1',
        r'loguru==0.2.5',
        r'motor==2.0.0',
        r'objgraph==3.4.1',
        r'Pillow==6.0.0',
        r'psutil==5.6.3',
        r'PyJWT==1.7.1',
        r'pytest==4.6.3',
        r'pytest-asyncio==0.10.0',
        r'Sphinx==2.1.1',
        r'SQLAlchemy==1.3.4',
        r'tornado==6.0.2',
        r'xlwt==1.3.0',
        r'xmltodict==0.12.0',
    ],
    classifiers=[
        r'Programming Language :: Python :: 3.6',
        r'Programming Language :: Python :: 3.7',
        r'License :: OSI Approved :: Apache Software License',
        r'Operating System :: POSIX :: Linux',
    ],
)
