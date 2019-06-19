# -*- coding: utf-8 -*-

import os
import sys

os.chdir(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(r'../'))

from hagworm.extend.asyncio.base import Launcher, Utils


async def main():

    Utils.log.info(r'waiting...')

    await Utils.sleep(5)

    Utils.log.info(r'finished')


if __name__ == r'__main__':

    Launcher().run(main())
