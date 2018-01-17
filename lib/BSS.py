"""Nuvla connector for UNICORE """

import hashlib
import os
import re
import time
import Utils

from BSSCommon import BSSBase

from slipstream.api.api import Api
from boto.s3.connection import S3Connection, Key, Bucket

CLOUD = 'exoscale'
CLOUD_CONN_NAME = 'exoscale-ch-gva'
CLOUD_CRED_NAME_PREF = 'hbp_mooc'
BUCKET_NAME_PREF = CLOUD_CRED_NAME_PREF
USERSPACE_RTP = 'userspace-endpoint'
MSG_NUVLA_USER_CRED_KEY = 'UC_NUVLA_CRED'

COMP_NAME = 'compute'

JOB_STATE_MAP = {'initializing': 'QUEUED',
                 'provisioning': 'QUEUED',
                 'executing': 'RUNNING',
                 'sendingreports': 'RUNNING',
                 'ready': 'COMPLETED',
                 'finalizing': 'COMPLETED',
                 'done': 'COMPLETED',
                 'cancelled': 'COMPLETED'
                 # '???': 'SUSPENDED',
                 }

def nuvla_creds(self, message):
    key_secret = re.search(r'^%s=.*$' % MSG_NUVLA_USER_CRED_KEY, message,)
    if key_secret:
        return key_secret.split(':')


class BSS(BSSBase):
    def _nuvla_creds(self, message):
        return nuvla_creds(message)

    @staticmethod
    def nuvla(message):
        key_secret = nuvla_creds(message)
        if key_secret:
            k, s = key_secret.split(':')
            m = hashlib.md5().update(k)
            cf = '/tmp/' + m.hexdigest() + '.txt'
            nuvla = Api('https://nuv.la', cookie_file=cf)
            nuvla.login_apikey(k, s)
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
        cfilter = "type='cloud-cred-%s' and name^='%s'" % (CLOUD,
                                                           CLOUD_CRED_NAME_PREF)
        creds = list(nuvla.get_cloud_credentials(cimi_filter=cfilter))
        if len(creds) < 1:
            raise Exception('Failed to find %s cloud credentials with '
                            'the name starting with %s.' % (
                                CLOUD, CLOUD_CRED_NAME_PREF))
        c = creds[0]
        return c.key, c.secret

    def _get_s3_connection(self, nuvla):
        key, secret = self._get_s3_creds(nuvla)
        return S3Connection(
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            host='sos.exo.io')

    def _put_files_to_s3(self, nuvla, message):
        """Returns S3 Key of the directory where the files were staged in.
        :param nuvla: slipstream.api.api.Api
        :param message: TSI message
        :return: boto.s3.key.Key - directory where files were staged.
        """
        s3 = self._get_s3_connection(nuvla)
        bucket_name = '%s_%s' % (
            BUCKET_NAME_PREF,
            hashlib.md5(nuvla.username.encode()).hexdigest())
        bucket = s3.create_bucket(bucket_name, policy='private')
        bucket_stage_dir = str(int(time.time() * 1000))
        in_dir = '%s/input/' % bucket_stage_dir
        out_dir = '%s/output/' % bucket_stage_dir
        for d in [in_dir, out_dir]:
            k = bucket.new_key(d)
            k.set_contents_from_string('')
        files = self._get_stagein_files(message)
        for f in files:
            fn = os.path.basename(f)
            key = bucket.new_key('%s%s' % (in_dir, fn))
            key.set_contents_from_filename(f)
        return Key(bucket, bucket_stage_dir + '/')

    def _download_files_from_s3(self, message, nuvla):
        bucket, dir_name = self._get_s3_scratch_dir(message, nuvla)
        local_path = Utils.extract_parameter(message, "USPACE")
        if not local_path:
            raise Exception('Failed to get local path to files as USPACE.')
        for k in bucket.list(prefix=('%s/output/' % dir_name)):
            if k.name.endswith('/'):
                continue
            fn = '%s/%s' % (local_path.rstrip('/'), os.path.basename(k.name))
            k.get_contents_to_filename(fn)

    def _get_s3_scratch_dir(self, message, nuvla):
        """Returns bucket and scratch directory name.
        :param message:
        :param nuvla:
        :return: (Bucket, str)
        """
        duid = Utils.extract_parameter(message, "BSSID")
        if not duid:
            raise Exception('Failed to get deployment uuid as BSSID.')
        s3_path = self._get_scratch_path(nuvla, duid)
        if not s3_path:
            raise Exception('Failed to get S3 path.')
        bucket_name, dir_name = s3_path.split('/')[0:2]
        s3 = self._get_s3_connection(nuvla)
        bucket = Bucket(s3, bucket_name)
        return bucket, dir_name

    def _delete_s3_scratch_space(self, message, nuvla):
        """Deletes scratch space on S3.
        """
        bucket, dir_name = self._get_s3_scratch_dir(message, nuvla)
        for k in bucket.list(dir_name):
            k.delete()

    def _apicred_params(self, message):
        return dict(zip(["api-key", "api-secret"], self._nuvla_creds(message)))

    def _get_scratch_path(self, nuvla, duid):
        param = '%s.1:%s' % (COMP_NAME, USERSPACE_RTP)
        return nuvla.get_deployment_parameter(duid, param, ignore_abort=True)

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
        try:
            s3_stage_path = self._put_files_to_s3(nuvla, message)
            compute_params = {
                USERSPACE_RTP: "%s/%s" % (s3_stage_path.bucket.name,
                                          s3_stage_path.name)}
            # FIXME: this will be removed when API key authn is available on VM.
            compute_params.update(self._apicred_params(message))
            params = {"compute": compute_params}
            dpl_id = nuvla.deploy(app, cloud={"compute": CLOUD_CONN_NAME},
                                  parameters=params, keep_running='never')
            connector.ok(str(dpl_id))
            return
        except Exception as ex:
            connector.failed(str(ex))

    def get_status_listing(self, message, connector, config, LOG):
        result = ['QSTAT']
        nuvla = self.nuvla(message)
        for dpl in nuvla.list_deployments(cloud=CLOUD_CONN_NAME):
            result.append('%s %s' % (dpl.id,
                                     self.convert_status(dpl.status)))
        connector.write_message('\n'.join(result) + '\n')

    def get_job_details(self, message, connector, config, LOG):
        duid = Utils.extract_parameter(message, "BSSID")
        if not duid:
            connector.failed('TSI_BSSID was not provided.')
            return
        nuvla = self.nuvla(message)
        state = self.convert_status(
            nuvla.get_deployment_parameter(duid, 'ss:state',
                                           ignore_abort=True))
        if state == 'COMPLETED':
            self._download_files_from_s3(message, nuvla)
            self._delete_s3_scratch_space(message, nuvla)
        connector.ok(state)

    def abort_job(self, message, connector, config, LOG):
        duid = Utils.extract_parameter(message, "BSSID")
        nuvla = self.nuvla(message)
        nuvla.terminate(duid)
        self._download_files_from_s3(message, nuvla)
        self._delete_s3_scratch_space(message, nuvla)
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
        return JOB_STATE_MAP.get(bss_state.lower(), 'UNKNOWN')

    def create_submit_script(self, message, config, LOG):
        return []
