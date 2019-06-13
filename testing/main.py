# -*- coding: utf-8 -*-

import os
import sys
import pytest

os.chdir(r'./testing')
sys.path.insert(0, os.path.abspath(r'../'))


if __name__ == r'__main__':

    pytest.main()
