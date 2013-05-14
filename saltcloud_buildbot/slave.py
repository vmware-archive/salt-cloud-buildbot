# -*- coding: utf-8 -*-
'''
    saltcloud_buildbot.slave
    ~~~~~~~~~~~~~~~~~~~~~~~~

    This is salt-cloud's buildbot slave implementation.

    :codeauthor: :email:`Pedro Algarvio (pedro@algarvio.me)`
    :copyright: © 2013 by the SaltStack Team, see AUTHORS for more details.
    :license: Apache 2.0, see LICENSE for more details.
'''

# Import python libs
import time
import random
import logging

# Import salt & salt-cloud libs
import salt.log
import salt.client
import salt.config
import salt.output
import salt.exceptions
import saltcloud.cloud
import saltcloud.config

# Setup the salt temporary logging
salt.log.setup_temp_logger()

# Import twisted libs
from twisted.internet import defer, reactor, threads

# Import buildbot libs
from buildbot.buildslave import AbstractLatentBuildSlave
from buildbot.interfaces import LatentBuildSlaveFailedToSubstantiate


log = logging.getLogger(__name__)


reactor.suggestThreadPoolSize(30)


class SaltCloudLatentBuildSlave(AbstractLatentBuildSlave):

    def __init__(
        self,
        # from AbstractLatentBuildSlave
        name,
        password,
        saltcloud_profile_name,
        max_builds=None,
        notify_on_missing=[],
        missing_timeout=60 * 20,
        build_wait_timeout=60 * 10,
        properties={},
        locks=None,
        # SaltCloudBuildSlave
        single_build=False,
        saltcloud_config='/etc/salt/cloud',
        saltcloud_vm_config='/etc/salt/cloud.profiles',
        saltcloud_master_config='/etc/salt/master',
        saltcloud_providers_config='/etc/salt/cloud.providers'
    ):

        if single_build:
            # Force VM shutdown
            build_wait_timeout = 0

        # Init parent
        AbstractLatentBuildSlave.__init__(
            self,
            name,
            password,
            max_builds,
            notify_on_missing,
            missing_timeout,
            build_wait_timeout,
            properties,
            locks
        )

        self._saltcloud_config = None
        self.saltcloud_vm_name = '{0}-buildbot-rnd{1:04d}'.format(
            self.slavename, random.randrange(0, 10001, 2)
        )
        self.saltcloud_config = saltcloud_config or '/etc/salt/cloud'
        self.saltcloud_vm_config = (
            saltcloud_vm_config or '/etc/salt/cloud.profiles'
        )
        self.saltcloud_master_config = (
            saltcloud_master_config or '/etc/salt/master'
        )
        self.saltcloud_providers_config = (
            saltcloud_providers_config or '/etc/salt/cloud.providers'
        )
        self.saltcloud_profile_name = saltcloud_profile_name

    def __load_saltcloud_config(self):
        if self._saltcloud_config is not None:
            return self._saltcloud_config

        # We want some early console debugging
        salt.log.setup_console_logger('debug')

        # Read/Parse salt-cloud configurations
        # salt master configuration
        master_config = self._salt_master_config = salt.config.master_config(
            self.saltcloud_master_config
        )

        # salt-cloud config
        cloud_config = saltcloud.config.cloud_config(
            self.saltcloud_config
        )

        # profiles configuration
        profiles_config = saltcloud.config.vm_profiles_config(
            self.saltcloud_vm_config
        )

        providers_config = saltcloud.config.cloud_providers_config(
            self.saltcloud_providers_config
        )

        config = master_config.copy()
        config.update(cloud_config)
        config['vm'] = profiles_config
        config['providers'] = providers_config

        # Update with some parsers cli defaults
        config.update({
            'map': None,
            'deploy': True,
            'parallel': False,
            'keep_tmp': False
        })

        # The profile we wish to run
        config['profile'] = self.saltcloud_profile_name

        # The machine name
        config['names'] = [self.saltcloud_vm_name]

        # Now configure logging respecting the configuration
        salt.log.setup_console_logger(
            config['log_level'],
            log_format=config['log_fmt_console'],
            date_format=config['log_datefmt']
        )

        loglevel = config.get(
            'log_level_logfile', config['log_level']
        )

        if config.get('log_fmt_logfile', None) is None:
            # Remove it from config so it inherits from log_fmt_console
            config.pop('log_fmt_logfile', None)

        logfmt = config.get(
            'log_fmt_logfile', config['log_fmt_console']
        )

        if config.get('log_datefmt', None) is None:
            # Remove it from config so it get's the default value bellow
            config.pop('log_datefmt', None)

        datefmt = config.get(
            'log_datefmt_logfile',
            config.get('log_datefmt', '%Y-%m-%d %H:%M:%S')
        )
        salt.log.setup_logfile_logger(
            config['log_file'],
            loglevel,
            log_format=logfmt,
            date_format=datefmt
        )
        for name, level in config['log_granular_levels'].items():
            salt.log.set_logger_level(name, level)

        self._saltcloud_config = config
        return self._saltcloud_config

    # AbstractLatentBuildSlave methods
    def start_instance(self, build):
        # responsible for starting instance that will try to connect with this
        # master. Should return deferred with either True (instance started)
        # or False (instance not started, so don't run a build here). Problems
        # should use an errback.
        if not self.output:  # None or ''
            self.output = ''
        return threads.deferToThread(self.__start_instance)

    def __start_instance(self):
        config = self.__load_saltcloud_config()

        for idx, vm_ in enumerate(config['vm']):
            if vm_['profile'] != config['profile']:
                continue

            minion_conf = saltcloud.config.get_config_value(
                'minion', vm_, config, default={}
            )

            # Setup the required slave grains to be used by the minion
            if 'master' not in minion_conf:
                import urllib2
                attempts = 5
                while attempts > 0:
                    try:
                        request = urllib2.urlopen('http://v4.ident.me/')
                        public_ip = request.read()
                        minion_conf['master'] = public_ip
                        self.output += (
                            '\nFound local public IP address, {0},  to be '
                            'used as the master address'.format(public_ip)
                        )
                        break
                    except urllib2.HTTPError:
                        log.warn(
                            'Failed to get the public IP for the master. '
                            'Remaining attempts: {0}'.format(
                                attempts
                            ),
                            # Show the traceback if the debug logging level is
                            # enabled
                            exc_info=log.isEnabledFor(logging.DEBUG)
                        )
                else:
                    msg = 'Failed to get the public IP for the master.'
                    self.output += '\n{0}'.format(msg)
                    raise LatentBuildSlaveFailedToSubstantiate(
                        config['profile'], msg
                    )

            # Set the buildbot slave name as a grain
            minion_conf.setdefault(
                'grains', {}).setdefault(
                    'buildbot', {})['slavename'] = self.slavename

            # Set the buildbot password as a grain
            minion_conf.setdefault(
                'grains', {}).setdefault(
                    'buildbot', {})['password'] = self.password

            # Update the virtual machines minion configuration
            config['vm'][idx]['minion'] = minion_conf

        mapper = saltcloud.cloud.Map(config)
        try:
            ret = mapper.run_profile()
            if not ret:
                msg = 'Failed to start {0} for slave {1}'.format(
                    self.saltcloud_vm_name,
                    self.slavename
                )
                log.error(msg)
                self.output += '\n{0}'.format(msg)
                raise LatentBuildSlaveFailedToSubstantiate(
                    self.saltcloud_vm_name, msg
                )
            msg = (
                'salt-cloud started VM {0} for slave {1}. '
                'Details:\n{2}'.format(
                    self.saltcloud_vm_name,
                    self.slavename,
                    salt.output.out_format(
                        ret[self.saltcloud_vm_name], 'pprint', config
                    )
                )
            )
            self.output += '\n{0}'.format(msg)
            log.info(msg)
        except Exception, err:
            msg = (
                'salt-cloud failed to start VM {0} for slave {1}. '
                'Details:\n{2}'.format(
                    self.saltcloud_vm_name,
                    self.slavename,
                    err
                )
            )
            log.error(msg, exc_info=True)
            self.output += '\n{0}'.format(msg)
            raise LatentBuildSlaveFailedToSubstantiate(
                self.saltcloud_vm_name, msg
            )

        # Let the minion connect back
        time.sleep(2)

        try:
            msg = 'Running \'state.highstate\' on the minion'
            self.output += '\n{0}'.format(msg)
            client = salt.client.LocalClient(
                mopts=self._salt_master_config
            )
        except Exception as err:
            msg (
                'Failed to instantiate the salt local client: {0}\n'.format(
                    err
                )
            )
            self.output += '\n{0}'.format(msg)
            log.error(msg, exc_info=True)

        try:
            attempts = 11
            while True:
                msg = (
                    'Publishing \'state.highstate\' job to {0}. '
                    'Attempts remaining {1}'.format(
                        self.saltcloud_vm_name,
                        attempts - 1
                    )
                )
                self.output += '\n{0}'.format(msg)
                try:
                    job = client.cmd_async(
                        [self.saltcloud_vm_name],
                        'state.highstate',
                        expr_form='list',
                    )
                    if not job:
                        attempts -= 1
                        if attempts < 1:
                            msg = (
                                'Failed to publish \'state.highstate\' job to '
                                '{0}. Returned empty response.'.format(
                                    self.saltcloud_vm_name
                                )
                            )
                            log.error(msg)
                            self.output += '\n{0}'.format(msg)
                            raise LatentBuildSlaveFailedToSubstantiate(
                                self.saltcloud_vm_name, msg
                            )
                        continue
                    break
                except salt.exceptions.SaltReqTimeoutError:
                    attempts -= 1
                    if attempts < 1:
                        msg = (
                            'Failed to publish \'state.highstate\' job to '
                            '{0} for slave {1}, timed out.'.format(
                                self.saltcloud_vm_name,
                                self.slavename
                            )
                        )
                        self.output += '\n{0}'.format(msg)
                        log.error(msg)
                        raise LatentBuildSlaveFailedToSubstantiate(
                            self.saltcloud_vm_name, msg
                        )
                    continue

            msg = 'Published job information: {0}'.format(job)
            self.output += '\n{0}'.format(msg)
            log.info(msg)

            # Let the job start
            time.sleep(6)

            attempts = 11
            while True:
                msg = (
                    'Checking if \'state.highstate\' is running on '
                    '{0}. Attempts remaining {1}'.format(
                        self.saltcloud_vm_name,
                        attempts - 1
                    )
                )
                self.output += '\n{0}'.format(msg)
                log.info(msg)
                try:
                    running = client.cmd(
                        [self.saltcloud_vm_name],
                        'saltutil.running',
                        expr_form='list'
                    )
                    msg = 'Running on the minion: {0}'.format(running)
                    log.info(msg)
                    self.output += '\n{0}'.format(msg)
                except salt.exceptions.SaltReqTimeoutError:
                    attempts -= 1
                    if attempts < 1:
                        msg = (
                            'Failed to check if state.highstate is running '
                            'on {0} for slave {1}, timed out'.format(
                                self.saltcloud_vm_name, self.slavename
                            )
                        )
                        log.error(msg)
                        self.output += '\n{0}'.format(msg)
                        raise LatentBuildSlaveFailedToSubstantiate(
                            self.saltcloud_vm_name, msg
                        )
                    continue

                if not running:
                    attempts -= 1
                    if attempts < 1:
                        msg = (
                            'state.highstate is apparently not running '
                            'on {0} for slave {1}, empty response'.format(
                                self.saltcloud_vm_name,
                                self.slavename
                            )
                        )
                        log.error(msg)
                        self.output += '\n{0}'.format(msg)
                        raise LatentBuildSlaveFailedToSubstantiate(
                            self.saltcloud_vm_name, msg
                        )
                    continue

                # Reset failed attempts
                if attempts < 11:
                    msg = 'Reseting failed attempts'
                    log.info(msg)
                    self.output += '\n{0}'.format(msg)
                    attempts = 11

                msg = 'Job is still running on {0}: {1}'.format(
                        self.saltcloud_vm_name, running
                )
                self.output += '\n{0}'.format(msg)
                log.info(msg)
                if not [job_details for job_details in
                        running.get(self.saltcloud_vm_name, [])
                        if job_details and job_details['jid'] == job]:
                    # Job is no longer running
                    msg = '\'state.highstate\' has completed on {0}'.format(
                        self.saltcloud_vm_name
                    )
                    self.output += '\n{0}'.format(msg)
                    log.info(msg)
                    break
                time.sleep(5)

            msg = 'state.highstate has apparently completed in {0}'.format(
                self.saltcloud_vm_name
            )
            self.output += '\n{0}'.format(msg)
            log.info(msg)

            # Let the minion settle
            time.sleep(2)
            msg = (
                'Getting \'state.highstate\' job information from {0}'.format(
                    self.saltcloud_vm_name
                )
            )
            self.output += '\n{0}'.format(msg)
            log.info(msg)

            highstate = client.get_full_returns(
                job, [self.saltcloud_vm_name], timeout=5
            )
            try:
                msg = (
                    'Output of running \'state.highstate\' on the {0} '
                    'minion({1}):\n{2}'.format(
                        self.slavename,
                        self.saltcloud_vm_name,
                        salt.output.out_format(
                            highstate[self.saltcloud_vm_name],
                            'highstate',
                            config
                        )
                    )
                )
                self.output += '\n{0}'.format(msg)
                log.info(msg)
            except Exception:
                msg = (
                    'Output of running \'state.highstate\' on the {0} '
                    'minion({1}):\n{2}'.format(
                        self.slavename,
                        self.saltcloud_vm_name,
                        salt.output.out_format(
                            highstate, 'pprint', config
                        )
                    )
                )
                self.output += '\n{0}'.format(msg)
                log.info(msg)

            if not highstate or 'Error' in highstate:
                msg = (
                    'Returned empty or error running state.highstate on '
                    '{0} for slave {1}: {2}'.format(
                        self.saltcloud_vm_name, highstate
                    )
                )
                self.output += '\n{0}'.format(msg)
                log.error(msg)
                raise LatentBuildSlaveFailedToSubstantiate(
                    self.saltcloud_vm_name, msg
                )

            if isinstance(highstate[self.saltcloud_vm_name]['ret'], list):
                # We got a list back!?
                msg = (
                    'Failed to run \'state.highstate\' on the {0} minion({1}).'
                    ' Highstate details:\n{2}'.format(
                        self.slavename,
                        self.saltcloud_vm_name,
                        highstate[self.saltcloud_vm_name]['ret']
                    ),
                )
                self.output += '\n{0}'.format(msg)
                log.error(msg)
                raise LatentBuildSlaveFailedToSubstantiate(
                    self.saltcloud_vm_name, msg
                )

            for step in highstate[self.saltcloud_vm_name]['ret'].values():
                if step['result'] is False:
                    try:
                        msg = 'The step {0[name]!r} failed!'.format(step)
                    except KeyError:
                        msg = (
                            'There was failure in a step. '
                            'Step details: {0}'.format(step)
                        )
                    log.error(msg)
                    self.output += '\n{0}'.format(msg)
                    raise LatentBuildSlaveFailedToSubstantiate(
                        self.saltcloud_vm_name, msg
                    )
            msg = (
                'state.highstate completed without any issues on {0}'.format(
                    self.saltcloud_vm_name
                )
            )
            self.output += '\n{0}'.format(msg)
            log.info(msg)
            return [self.saltcloud_vm_name, self.slavename]
        except Exception, err:
            msg = (
                'Failed to run \'state.highstate\' on the {0} minion({1}). '
                'Details:\n{2}'.format(
                    self.slavename,
                    self.saltcloud_vm_name,
                    err
                )
            )
            log.error(
                msg,
                # Show the traceback if the debug logging level is enabled
                exc_info=log.isEnabledFor(logging.DEBUG)
            )
            raise LatentBuildSlaveFailedToSubstantiate(
                self.saltcloud_vm_name, msg
            )

    def stop_instance(self, fast=False):
        # responsible for shutting down instance.
        return threads.deferToThread(self.__stop_instance)

    def __stop_instance(self):
        config = self.__load_saltcloud_config()
        mapper = saltcloud.cloud.Map(config)
        try:
            ret = mapper.destroy(config['names'])
            msg = (
                'salt-cloud stopped VM {0} for slave {1}. '
                'Details:\n{2}'.format(
                    self.saltcloud_vm_name,
                    self.slavename,
                    salt.output.out_format(ret, 'pprint', config)
                )
            )
            log.info(msg)
            self.output += '\n{0}'.format(msg)
            self.output = None
            return True
        except Exception, err:
            msg = (
                'salt-cloud failed to stop VM {0} for slave {1}. '
                'Details:\n{2}'.format(
                    self.saltcloud_vm_name,
                    self.slavename,
                    err
                )
            )
            log.error(msg, exc_info=True)
            raise
