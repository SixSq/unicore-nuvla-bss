import os
import shutil
import tempfile
import unittest

from BSS import BSS
import pytest

pytestmark = pytest.mark.local


class TestBSSTorque(unittest.TestCase):

    def test_get_app_uri(self):
        msg = """#!/bin/bash
#TSI_SUBMIT
#TSI_SCRIPT

"""
        assert None is BSS._get_app_uri(msg)

        msg = """#!/bin/bash
#TSI_SUBMIT
#TSI_SCRIPT
SOME_VAR=empty
UC_EXECUTABLE=foo/bar
UC_USERDN=abc
"""
        assert 'foo/bar' == BSS._get_app_uri(msg)

    def test_get_stagein_files(self):
        path = tempfile.mkdtemp()
        files = set()
        for i in range(2):
            f = os.path.join(path, '%s.txt' % i)
            open(f, 'a').close()
            files.update([f])
        try:
            msg = "#TSI_USPACE %s\n" % path
            files_chk = set(BSS._get_stagein_files(msg))
            assert 0 == len(files ^ files_chk)
        finally:
            shutil.rmtree(path)
