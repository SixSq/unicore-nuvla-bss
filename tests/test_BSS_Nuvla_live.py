from BSS import BSS
import pytest

pytestmark = pytest.mark.live

nuvla_user = ''
nuvla_pass = ''

cloud_key = 'xxx'
cloud_secret = 'yyy'


def test_get_cloud_creds():
    nuvla = BSS.nuvla("#TSI_IDENTITY %s:%s\n" % (nuvla_user, nuvla_pass))
    bss = BSS()
    key, secret = bss._get_s3_creds(nuvla)
    assert cloud_key == key
    assert cloud_secret == secret
