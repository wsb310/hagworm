# -*- coding: utf-8 -*-

import os
import sys

import argparse
import py_compile


def compile(file_path, cfile_path=None):
    """PYC编译函数
    """

    for root, _, files in os.walk(file_path):

        for _file in files:

            _, ext_name = os.path.splitext(_file)

            if ext_name != r'.py':
                continue

            if cfile_path is None:

                dest_path = file_path

            else:

                dest_path = root.replace(file_path, cfile_path)

                if not os.path.exists(dest_path):
                    os.makedirs(dest_path)

            ori_path = os.path.join(root, _file)
            dest_path = os.path.join(dest_path, _file) + r'c'

            py_compile.compile(ori_path, dest_path, optimize=2)

            sys.stdout.write(f'{ori_path} => {dest_path}\n')


def main():

    result = 0

    parser = argparse.ArgumentParser()

    parser.add_argument(r'-i', r'--input', default=r'./', dest=r'input')
    parser.add_argument(r'-o', r'--output', default=None, dest=r'output')

    args = parser.parse_args()

    try:
        compile(args.input, args.output)
    except Exception as error:
        sys.stderr.write(f'{error}\n')

    return result


if __name__ == r'__main__':

    sys.exit(main())
