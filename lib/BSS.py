"""Nuvla connector for UNICORE """

import hashlib
import os
import re
import time
import Utils

from BSSCommon import BSSBase

from slipstream.api.api import Api
from boto.s3.connection import S3Connection
from boto.s3.key import Key

CLOUD = 'exoscale'
CLOUD_CRED_NAME_PREF = 'hbp_mooc'
BUCKET_NAME_PREF = CLOUD_CRED_NAME_PREF


class BSS(BSSBase):

    @staticmethod
    def nuvla(message):
        key_secret = Utils.extract_parameter(message, "IDENTITY")
        if key_secret:
            nuvla = Api('https://nuv.la')
            nuvla.login_apikey(*key_secret.split(':'))
            return nuvla
        else:
            raise Exception('No key/secret provided via IDENTITY.')

    @staticmethod
    def _get_app_uri(message):
        app = re.search(r'^UC_EXECUTABLE=.*$', message, re.MULTILINE)
        if app:
            return app.group(0).split('=')[1]
        else:
            return None

    @staticmethod
    def _get_stagein_files(message):
        """Only files. Directories are skipped.
        """
        path = Utils.extract_parameter(message, "USPACE")
        return [os.path.join(path, f) for f in os.listdir(path)
                if os.path.isfile(os.path.join(path, f))]

    def get_variant(self):
        return "nuvla"

    def _get_s3_creds(self, nuvla):
        creds = list(nuvla.get_cloud_credentials(
            cimi_filter="type='cloud-cred-%s'" % CLOUD))
        if len(creds) < 1:
            raise Exception('Failed to find cloud creds for %s.' % CLOUD)
        for c in creds:
            if c.name.startswith(CLOUD_CRED_NAME_PREF):
                return c.key, c.secret
        raise Exception('Failed to find %s cloud credentials with '
                        'the name starting with %s.' % (CLOUD, CLOUD_CRED_NAME_PREF))

    def _get_s3_connection(self, nuvla):
        key, secret = self._get_s3_creds(nuvla)
        return S3Connection(
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            host='sos.exo.io')

    def _stage_in_to_s3(self, nuvla, message):
        """Returns S3 Key of the directory where the files where staged in.
        :param nuvla: slipstream.api.api.Api
        :param message: TSI message
        :return: boto.s3.key.Key
        """
        s3 = self._get_s3_connection(nuvla)
        bucket_name = '%s_%s' % (
            BUCKET_NAME_PREF,
            hashlib.md5(nuvla.username.encode()).hexdigest())
        bucket = s3.create_bucket(bucket_name, policy='private')
        bucket_stage_dir = str(int(time.time() * 1000))
        files = self._get_stagein_files(message)
        for f in files:
            fn = os.path.basename(f)
            key = Key(bucket, '%s/%s' % (bucket_stage_dir, fn))
            key.set_contents_from_filename(f)
        return Key(bucket, bucket_stage_dir)

    def submit(self, message, connector, config, LOG):
        try:
            nuvla = self.nuvla(message)
        except Exception as ex:
            connector.failed('Failed to authenticate to Nuvla: %s' % str(ex))
            return
        app = self._get_app_uri(message)
        if not app:
            connector.failed('No application URI provided.')
            return
        self._stage_in_to_s3(nuvla, message)
        try:
            dpl_id = nuvla.deploy(app)
            connector.ok(dpl_id)
            return
        except Exception as ex:
            connector.failed(str(ex))

    def get_status_listing(self, message, connector, config, LOG):
        result = ['QSTAT']
        nuvla = self.nuvla(message)
        for dpl in nuvla.list_deployments():
            result.append('%s %s' % (dpl.uuid, dpl.status))
        connector.write_message('\n'.join(result) + '\n')

    def get_job_details(self, message, connector, config, LOG):
        dpl_uuid = Utils.extract_parameter(message, "BSSID")
        nuvla = self.nuvla(message)
        state = nuvla.get_deployment_parameter(dpl_uuid, 'ss:state',
                                         ignore_abort=True)
        connector.ok(state)

    def abort_job(self, message, connector, config, LOG):
        bssid = Utils.extract_parameter(message, "BSSID")
        nuvla = self.nuvla()
        nuvla.terminate(bssid)
        connector.ok()

    cancel_job = abort_job

    def hold_job(self, message, connector, config, LOG):
        output = "hold_job is not applicable for %s BSS" % self.get_variant()
        connector.ok(output)

    def resume_job(self, message, connector, config, LOG):
        output = "resume_job is not applicable for %s BSS" % self.get_variant()
        connector.ok(output)

    def get_budget(self, message, connector, config, LOG):
        connector.ok("USER -1\n")

    def convert_status(self, bss_state):
        """ converts BSS status to UNICORE status """
        ustate = "UNKNOWN"
        if bss_state.lower() in ["", "running"]:
            ustate = "RUNNING"
        elif bss_state == "T":
            ustate = "SUSPENDED"
        return ustate
    def create_submit_script(self, message, config, LOG):
        return []

