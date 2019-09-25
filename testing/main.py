# -*- coding: utf-8 -*-

import os
import sys
import pytest

os.chdir(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(r'../'))


if __name__ == r'__main__':

    pytest.main()
