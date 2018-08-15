from time import time
import re
import os
import Utils
from abc import ABCMeta
from abc import abstractmethod


class BSSBase(object):
    """Base class for batch system specific functions:
        - submit jobs
        - get status listing
        - parse the status listing
        - job control (abort, hold, resume, get details, ...)

    Check the manual for advice on how to create a custom version.
    """

    __metaclass__ = ABCMeta

    def get_variant(self):
        return "<base>"

    def cleanup(self, config):
        """ cleanup child processes """
        children = config.get('tsi.NOBATCH.children')
        for child in children:
            return_code = child.poll()
            if return_code is not None:
                children.remove(child)

    defaults = {
        'tsi.qstat_cmd': 'ps -e -os,args',
        'tsi.abort_cmd': 'SID=$(ps -e -osid,args | grep "nice .* ./UNICORE_Job_%s" | grep -v "grep " | egrep -o "^\s*([0-9]+)" ); pkill -SIGTERM -s $SID',
    }

    def init(self, config, LOG):
        """ setup default commands if necessary """
        for key in self.defaults:
            if config.get(key) is None:
                value = self.defaults[key]
                config[key] = value
                LOG.info("Parameter not set: '%s', will use default '%s'",
                         key, value)
        # check if BSS commands are accessible
        if config.get('tsi.testing') is not True:
            (success, output) = Utils.run_command(config['tsi.qstat_cmd'])
            if not success:
                msg = "Could not run command to check job statuses! " \
                      "Please check that the correct TSI is installed, and " \
                      "check the configuration of 'tsi.qstat_cmd' : %s" % output
                LOG.error(msg)
                raise RuntimeError(msg)
        # for storing child process PIDs
        children = config.get('tsi.NOBATCH.children')
        if children is None:
            config['tsi.NOBATCH.children'] = []

            
    @abstractmethod
    def create_submit_script(self, message, config, LOG):
        return []

    def submit(self, message, connector, config, LOG):
        """Submit a script
        """
        LOG.debug("Submitting a script.")
        message = Utils.expand_variables(message)

        submit_cmds = self.create_submit_script(message, config, LOG)
        uspace_dir = Utils.extract_parameter(message, "USPACE_DIR")

        # create unique name for the files used in this job submission
        submit_id = str(int(time() * 1000))
        userjob_file_name = "UNICORE_Job_%s" % submit_id
        submit_file_name = "bss_submit_%s" % submit_id

        # fully qualified path to the script which is to be executed
        submit_cmds.append(uspace_dir + "/" + userjob_file_name)

        # CD into the uspace
        os.chdir(uspace_dir)

        # Write the job script to a file
        with open(userjob_file_name, "w") as job:
            job.write(u"" + message)
        Utils.addperms(userjob_file_name, 0o770)

        # Write the submit commands to a file
        with open(submit_file_name, "w") as submit:
            for line in submit_cmds:
                submit.write(line + u"\n")
        Utils.addperms(submit_file_name, 0o770)

        # now run the job submission command
        cmd = config['tsi.submit_cmd'] + " " + submit_file_name

        (success, reply) = Utils.run_command(cmd)
        if not success:
            connector.failed(reply)
        else:
            LOG.info("Job submission result: %s" % reply)
            job_id = self.extract_job_id(reply)
            if job_id is not None:
                connector.write_message(job_id)
            else:
                connector.failed("Submit failed? Submission result:" + reply)

                # cd back to a neutral place
        os.chdir("/tmp")

    def extract_job_id(self, submit_result):
        """ extracts the job ID after submission to the BSS """
        # expect "<blah>NNN<blah> ...", extract the 'NNN'
        job_id = None
        m = re.search(r"\D*(\d+)\D*", submit_result)
        if m is not None:
            job_id = m.group(1)
        return job_id

    def extract_info(self, qstat_line):
        raise RuntimeError("Method not implemented!")

    def convert_status(self, bss_state):
        raise RuntimeError("Method not implemented!")

    __ustates = ["COMPLETED", "QUEUED", "SUSPENDED", "RUNNING"]

    def parse_status_listing(self, qstat_result):
        """ Does the actual parsing of the status listing. """
        states = {}
        for line in qstat_result.splitlines():
            (bssid, state, queue_name) = self.extract_info(str(line))
            if bssid is None:
                continue
            ustate = self.convert_status(state)
            if states.get(bssid, None) is None:
                states[bssid]=(ustate,queue_name)
            else:
                have_state,_ = states[bssid]
                if self.__ustates.index(ustate)>self.__ustates.index(have_state):
                    states[bssid]=(ustate,queue_name)
 
        # generate reply to UNICORE/X
        result = "QSTAT\n"
        for bssid in states:
            ustate, queue_name = states[bssid]
            result += " %s %s %s\n" % (bssid, ustate, queue_name)
        return result

    def get_status_listing(self, message, connector, config, LOG):
        """ Get info about all the batch jobs and parses it.
        """
        qstat_cmd = config["tsi.qstat_cmd"]
        (success, qstat_output) = Utils.run_command(qstat_cmd)
        if not success:
            connector.failed(qstat_output)
            return
        result = self.parse_status_listing(qstat_output)
        connector.write_message(result)

    def get_job_details(self, message, connector, config, LOG):
        bssid = Utils.extract_parameter(message, "BSSID")
        cmd = config["tsi.details_cmd"] + " " + bssid
        Utils.run_and_report(cmd, connector)

    def abort_job(self, message, connector, config, LOG):
        bssid = Utils.extract_parameter(message, "BSSID")
        cmd = config["tsi.abort_cmd"] % bssid
        Utils.run_and_report(cmd, connector)

    def hold_job(self, message, connector, config, LOG):
        bssid = Utils.extract_parameter(message, "BSSID")
        cmd = config["tsi.hold_cmd"] + " " + bssid
        Utils.run_and_report(cmd, connector)

    def resume_job(self, message, connector, config, LOG):
        bssid = Utils.extract_parameter(message, "BSSID")
        cmd = config["tsi.resume_cmd"] + " " + bssid
        Utils.run_and_report(cmd, connector)

    def get_budget(self, message, connector, config, LOG):
        """ Gets the remaining compute time for the
        current user on this resource in core-hours.
        Returns "-1" if not available or applicable.
        """
        connector.ok("USER -1\n")
