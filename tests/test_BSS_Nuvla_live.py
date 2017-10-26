import os
import shutil
import tempfile
import pytest
from BSS import BSS, BUCKET_NAME_PREF
from boto.s3.key import Key

pytestmark = pytest.mark.live

nuvla_apikey = ''
nuvla_apisecret = ''

cloud_key = ''
cloud_secret = ''


def test_get_cloud_creds():
    nuvla = BSS.nuvla("#TSI_IDENTITY %s:%s\n" % (nuvla_apikey, nuvla_apisecret))
    bss = BSS()
    key, secret = bss._get_s3_creds(nuvla)
    assert cloud_key == key
    assert cloud_secret == secret


def test_stage_in_to_s3():
    path = tempfile.mkdtemp()
    files = set()
    for i in range(2):
        f = os.path.join(path, '%s.txt' % i)
        open(f, 'a').close()
        files.update([f])
    s3_path = None
    try:
        message = """
#TSI_IDENTITY %s:%s
#TSI_USPACE %s
""" % (nuvla_apikey, nuvla_apisecret, path)
        nuvla = BSS.nuvla(message)
        bss = BSS()
        s3_path = bss._stage_in_to_s3(nuvla, message)
        assert isinstance(s3_path, Key)
        assert s3_path.bucket.name.startswith(BUCKET_NAME_PREF)
    finally:
        shutil.rmtree(path)
        try:
            if s3_path:
                for e in s3_path.bucket.list(prefix=s3_path.name):
                    e.delete()
        except:
            pass
