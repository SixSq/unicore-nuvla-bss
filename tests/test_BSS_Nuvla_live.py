import os
import re
import time
import shutil
import tempfile
import pytest
from BSS import BSS, BUCKET_NAME_PREF, JOB_STATE_MAP
from MockConnector import MockConnector
from boto.s3.key import Key

uuid_re = re.compile(
    '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')

pytestmark = pytest.mark.live

NUVLA_APP = 'konstan/hbp_mooc/compute-cluster'

nuvla_apikey = ''
nuvla_apisecret = ''

cloud_key = ''
cloud_secret = ''

need_nuvla_creds = pytest.mark.skipif(not all([nuvla_apikey, nuvla_apisecret]),
                                      reason="requires nuvla credentials")
need_cloud_creds = pytest.mark.skipif(not all([cloud_key, cloud_secret]),
                                      reason="requires cloud credentials")


@need_nuvla_creds
@need_cloud_creds
def test_get_cloud_creds():
    nuvla = BSS.nuvla("#TSI_IDENTITY %s:%s\n" % (nuvla_apikey, nuvla_apisecret))
    bss = BSS()
    key, secret = bss._get_s3_creds(nuvla)
    assert cloud_key == key
    assert cloud_secret == secret


def cleanup_s3_staging(s3_path):
    try:
        if s3_path:
            for e in s3_path.bucket.list(prefix=s3_path.name):
                e.delete()
    except:
        pass


@need_nuvla_creds
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
        s3_path = bss._put_files_to_s3(nuvla, message)
        assert isinstance(s3_path, Key)
        assert s3_path.bucket.name.startswith(BUCKET_NAME_PREF)
    finally:
        shutil.rmtree(path)
        cleanup_s3_staging(s3_path)


def test_workflow():
    path = tempfile.mkdtemp()
    files = set()
    for i in range(2):
        f = os.path.join(path, '%s.txt' % i)
        open(f, 'a').close()
        files.update([f])
    s3_path = None
    message = """#!/bin/bash
#TSI_IDENTITY %s:%s
#TSI_USPACE %s
#TSI_SCRIPT
UC_EXECUTABLE=%s
""" % (nuvla_apikey, nuvla_apisecret, path, NUVLA_APP)
    print 'initial message:', message
    try:

        bss = BSS()

        # submit deployment.
        connector = MockConnector(None, None, None, None, None)
        bss.submit(message, connector, None, None)

        # assert deployment UUID was set on the TSI connector.
        duid = ''
        for line in connector.control_out.getvalue().split('\n'):
            if uuid_re.match(line):
                duid = line
        assert len(duid) == 36

        # Insert #TSI_BSSID to the message.
        message_ = filter(lambda x: len(x) > 0, message.split('\n'))
        message_.insert(1, "#TSI_BSSID %s" % duid)
        message = '\n'.join(message_)
        print 'updated message:', message

        connector = MockConnector(None, None, None, None, None)
        bss.get_status_listing(message, connector, None, None)
        assert duid in connector.control_out.getvalue()

        # set s3_path for the later cleanup.
        nuvla = BSS.nuvla(message)
        scratch_path = bss._get_scratch_path(nuvla, duid)
        bucket_name, dir_name = scratch_path.split('/')[0:2]
        s3 = bss._get_s3_connection(nuvla)
        bucket = s3.create_bucket(bucket_name)
        s3_path = Key(bucket, dir_name)
        print "S3 path", s3_path.name

        # wait for Done state
        while True:
            connector = MockConnector(None, None, None, None, None)
            bss.get_job_details(message, connector, None, None)
            state = connector.control_out.getvalue().split('\n')[1]
            assert state in JOB_STATE_MAP.values()
            print 'Deployment %s state %s' % (duid, state)
            if state == 'COMPLETED':
                print 'Job completed.'
                break
            print 'Sleeping 5 sec.'
            time.sleep(5)

        assert os.path.exists('%s/%s' % (path, 'result.txt'))
        assert [] == list(bucket.list(dir_name))

    finally:
        pass
        shutil.rmtree(path)
        cleanup_s3_staging(s3_path)
