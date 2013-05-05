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
from buildbot import interfaces


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
                    raise interfaces.LatentBuildSlaveFailedToSubstantiate(
                        config['profile'],
                        'Failed to get the public IP for the master.'
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
            log.info(
                'salt-cloud started VM {0} for slave {1}. '
                'Details:\n{2}'.format(
                    self.saltcloud_vm_name,
                    self.slavename,
                    salt.output.out_format(ret, 'pprint', config)
                )
            )
            if not ret:
                return False
        except Exception, err:
            log.error(
                'salt-cloud failed to start VM {0} for slave {1}. '
                'Details:\n{2}'.format(
                    self.saltcloud_vm_name,
                    self.slavename,
                    err
                ),
                exc_info=True
            )
            return False

        # Let the minion connect back
        time.sleep(2)

        try:
            log.info('Running \'state.highstate\' on the minion')
            job_logger = logging.getLogger(
                '{0}.saltclient'.format(__name__)
            )
            client = salt.client.LocalClient(
                mopts=self._salt_master_config
            )

            attempts = 6
            while True:
                log.info(
                    'Publishing \'state.highstate\' job to {0}. '
                    'Attempts remaining {1}'.format(
                        self.saltcloud_vm_name,
                        attempts - 1
                    )
                )
                try:
                    job = client.run_job(
                        [self.saltcloud_vm_name],
                        'state.highstate',
                        expr_form='list',
                        timeout=9999999999999999,
                    )
                    if not job:
                        attempts -= 1
                        if attempts < 1:
                            log.error(
                                'Failed to publish \'state.highstate\' job to '
                                '{0}. '.format(self.saltcloud_vm_name)
                            )
                            return False
                        continue
                    break
                except salt.exceptions.SaltReqTimeoutError:
                    attempts -= 1
                    if attempts < 1:
                        log.error(
                            'Failed to publish \'state.highstate\' job to '
                            '{0}. '.format(self.saltcloud_vm_name)
                        )
                        return False
                    continue

            # Let the job start
            time.sleep(2)

            attempts = 6
            completed = False
            while True:
                try:
                    log.info(
                        'Checking if \'state.highstate\' is running on '
                        '{0}. Attempts remaining {1}'.format(
                            self.saltcloud_vm_name,
                            attempts - 1
                        )
                    )
                    running = client.cmd(
                        [self.saltcloud_vm_name],
                        'saltutil.is_running',
                        arg=('state.highstate',),
                        expr_form='list'
                    )
                except salt.exceptions.SaltReqTimeoutError:
                    attempts -= 1
                    if attempts < 1:
                        log.error(
                            'Failed to check if state.highstate is running '
                            'on {0}'.format(self.saltcloud_vm_name)
                        )
                        return False
                    continue

                # Reset failed attempts
                attempts = 6

                job_logger.debug('IS RUNNING: {0}'.format(running))
                if running and completed:
                    # False positive, reset completed flag
                    completed = False
                elif not running and not completed:
                    # Let's try not to get false positives
                    completed = True
                elif not running and completed:
                    # Job is no longer running
                    log.info(
                        '\'state.highstate\' has completed on {0}'.format(
                            self.saltcloud_vm_name
                        )
                    )
                    break
                time.sleep(5)

            # Let the minion settle
            time.sleep(1)
            log.info(
                'Getting \'state.highstate\' job information from {0}'.format(
                    self.saltcloud_vm_name
                )
            )
            ret = client.get_full_returns(
                job['jid'],
                [self.saltcloud_vm_name],
                timeout=5
            )
            try:
                log.info(
                    '1- Output of running \'state.highstate\' on the {0} '
                    'minion({1}):\n{2}'.format(
                        self.slavename,
                        self.saltcloud_vm_name,
                        salt.output.out_format(ret, 'highstate', config)
                    )
                )
            except AttributeError:
                try:
                    log.info(
                        '2 -Output of running \'state.highstate\' on the {0} '
                        'minion({1}):\n{2}'.format(
                            self.slavename,
                            self.saltcloud_vm_name,
                            salt.output.out_format(
                                ret['ret'], 'highstate', config
                            )
                        )
                    )
                except (AttributeError, KeyError):
                    log.info(
                        '3 -Output of running \'state.highstate\' on the {0} '
                        'minion({1}):\n{2}'.format(
                            self.slavename,
                            self.saltcloud_vm_name,
                            salt.output.out_format(ret, 'pprint', config)
                        )
                    )

            if not ret or 'Error' in ret:
                return False
            return True
        except Exception, err:
            log.error(
                'Failed to run \'state.highstate\' on the {0} minion({1}). '
                'Details:\n{2}'.format(
                    self.slavename,
                    self.saltcloud_vm_name,
                    err
                ),
                # Show the traceback if the debug logging level is enabled
                exc_info=log.isEnabledFor(logging.DEBUG)
            )
            return False

    def stop_instance(self, fast=False):
        # responsible for shutting down instance.
        return threads.deferToThread(self.__stop_instance)

    def __stop_instance(self):
        config = self.__load_saltcloud_config()
        mapper = saltcloud.cloud.Map(config)
        try:
            ret = mapper.destroy(config['names'])
            log.info(
                'salt-cloud stopped VM {0} for slave {1}. '
                'Details:\n{2}'.format(
                    self.saltcloud_vm_name,
                    self.slavename,
                    salt.output.out_format(ret, 'pprint', config)
                )
            )
            return True
        except Exception, err:
            log.error(
                'salt-cloud failed to stop VM {0} for slave {1}. '
                'Details:\n{2}'.format(
                    self.saltcloud_vm_name,
                    self.slavename,
                    err
                ),
                exc_info=True
            )
            return False
