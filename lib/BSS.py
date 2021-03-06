"""Nuvla connector for UNICORE """

import hashlib
import os
import re
import time
import sys
import Utils

from BSSCommon import BSSBase

from slipstream.api.api import Api
from boto.s3.connection import S3Connection, Key, Bucket

CLOUD = 'exoscale'
CLOUD_CONN_NAME = 'exoscale-ch-gva'
CLOUD_CRED_NAME_PREF = 'hbp-mooc'
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


class BSS(BSSBase):

    @staticmethod
    def check_params(messages):
        params = BSS._nuvla_parameter_dict(messages)
        return params

    @staticmethod
    def _nested_set(dic, keys, value):
        for key in keys[:-1]:
            dic = dic.setdefault(key, {})
        dic[keys[-1]] = value

    @staticmethod
    def _nuvla_parameter(options, line):
        """
        Extracts a value that is given in the form 'NUVLA__<node>__<parameter name>="<value>"'
        from the line. If the '__<node>' part is not provided the node value defaults to
        'compute'.  Updates the options with the parameter set.
        """
        result = re.search(r'^NUVLA__(.+)__(.+)="(.+)";.*$', line)
        if result is not None:
            BSS._nested_set(options, [result.group(1), result.group(2)], result.group(3))
        else:
            result = re.search(r'^NUVLA__(.+)="(.+)";.*$', line)
            if result is not None:
                BSS._nested_set(options, [COMP_NAME, result.group(1)], result.group(2))
            
        return options

    @staticmethod
    def _nuvla_parameter_dict(message):
        result = {}
        for line in message.splitlines():
            BSS._nuvla_parameter(result, line)
        return result
        
    @staticmethod
    def nuvla(message):
        token = Utils.extract_parameter(message, "CREDENTIALS")

        if token:
            m = hashlib.md5()
            m.update(token)
            cf = '/tmp/' + m.hexdigest() + '.txt'
            nuvla = Api('https://nuv.la', cookie_file=cf)
            nuvla.login({"href": "session-template/mitreid-token-hbp",
                         "token": token})
            return nuvla
        else:
            raise Exception('No token or invalid token provided in TSI_CREDENTIALS.\n')

    @staticmethod
    def _get_app_uri(message):
        app = re.search(r'^UC_EXECUTABLE=\'(.*)\';', message, re.MULTILINE)
        print(app)
        if app:
            print(app.group(1))
            return app.group(1)
        else:
            return None

    @staticmethod
    def _get_stagein_files(message):
        """Only files. Directories are skipped.
        """
        path = Utils.extract_parameter(message, "USPACE_DIR")
        return [os.path.join(path, f) for f in os.listdir(path)
                if os.path.isfile(os.path.join(path, f))]

    def get_variant(self):
        return "nuvla"

    def _get_s3_creds(self, nuvla):
        cfilter = "type='cloud-cred-%s' and connector/href^='connector/%s'" % (CLOUD,
                                                           CLOUD_CONN_NAME)
        creds = list(nuvla.get_cloud_credentials(cimi_filter=cfilter))
        if len(creds) < 1:
            raise Exception('Failed to find %s cloud credentials with '
                            'the name starting with %s.' % (
                                CLOUD, CLOUD_CONN_NAME))
        c = creds[0]
        return c.key, c.secret

    def _get_s3_connection(self, nuvla):
        key, secret = self._get_s3_creds(nuvla)
        return S3Connection(
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            host='sos-ch-dk-2.exo.io')

    def _put_files_to_s3(self, nuvla, message):
        """Returns S3 Key of the directory where the files were staged in.
        :param nuvla: slipstream.api.api.Api
        :param message: TSI message
        :return: boto.s3.key.Key - directory where files were staged.
        """
        s3 = self._get_s3_connection(nuvla)
        bucket_name = '%s-%s' % (
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
        local_path = Utils.extract_parameter(message, "USPACE_DIR")
        if not local_path:
            raise Exception('Failed to get local path to files as USPACE.')
        for k in bucket.list(prefix=('%s/output/' % dir_name)):
            if k.name.endswith('/'):
                continue
            fn = '%s/%s' % (local_path.rstrip('/'), os.path.basename(k.name))
            k.get_contents_to_filename(fn)
        # create exit code file expected by UNICORE
        exit_code = '%s/%s' % (local_path.rstrip('/'), "UNICORE_SCRIPT_EXIT_CODE")
        with open(exit_code, "w") as f:
            f.write('0\n')

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

    def _get_scratch_path(self, nuvla, duid):
        param = '%s.1:%s' % (COMP_NAME, USERSPACE_RTP)
        return nuvla.get_deployment_parameter(duid, param, ignore_abort=True)

    def submit(self, message, connector, config, LOG):
        try:
            nuvla = self.nuvla(message)
            LOG.info('successfully authenticated with Nuvla')
        except Exception as ex:
            connector.failed('Failed to authenticate to Nuvla: %s' % str(ex))
            return
        try:
            app = self._get_app_uri(message)
            if not app:
                connector.failed('No application URI provided.')
                return
            LOG.info('found application URI: %s' % app)

            params = BSS._nuvla_parameter_dict(message)

            s3_stage_path = self._put_files_to_s3(nuvla, message)
            s3_stage_path_str =  "%s/%s" % (s3_stage_path.bucket.name,
                                            s3_stage_path.name)
            BSS._nested_set(params, [COMP_NAME, USERSPACE_RTP], s3_stage_path_str)
            
            LOG.info("parameters: %s" % str(params))

            # use default cloud for all nodes, old value was:
            # {COMP_NAME: CLOUD_CONN_NAME}
            cloud_params = {}
            
            dpl_id = nuvla.deploy(app, cloud=cloud_params,
                                  parameters=params, keep_running='never')
            LOG.info("Submitted to Nuvla with id %s" % str(dpl_id))
            #connector.ok()
            connector.write_message(str(dpl_id))
            return
        except:
            LOG.exception("Error submitting to NUVLA")
            connector.failed(str(sys.exc_info()[1]))

    def get_status_listing(self, message, connector, config, LOG):
        result = ['QSTAT']
        nuvla = self.nuvla(message)
        for dpl in nuvla.list_deployments(cloud=CLOUD_CONN_NAME):
            result.append('%s %s' % (dpl.id,
                                     self.convert_status(dpl.status)))
        message = '\n'.join(result) + '\n'
        LOG.info(message)
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
        LOG.info("state: %s, %s" % (duid, state))
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
