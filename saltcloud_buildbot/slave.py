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
from twisted.internet import reactor, threads

# Import buildbot libs
from buildbot.buildslave import AbstractLatentBuildSlave
from buildbot.interfaces import LatentBuildSlaveFailedToSubstantiate


log = logging.getLogger(__name__)


reactor.suggestThreadPoolSize(30)


class SaltCloudLatentBuildSlave(AbstractLatentBuildSlave):

    output = None

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
        config = saltcloud.config.cloud_config(
            # salt-cloud config
            self.saltcloud_config,
            # salt master configuration
            master_config_path=self.saltcloud_master_config,
            # providers configuration
            providers_config_path=self.saltcloud_providers_config,
            # profiles configuration
            vm_config_path=self.saltcloud_vm_config
        )

        # Update with some parsers cli defaults
        config.update({
            'map': None,
            'deploy': True,
            'parallel': False,
            'keep_tmp': False
        })

        # Now configure logging respecting the configuration
        # First console logging
        cli_log_fmt = 'cli_salt_cloud_log_fmt'
        if cli_log_fmt in config and not config.get(cli_log_fmt):
            # Remove it from config so it inherits from log_fmt_console
            config.pop(cli_log_fmt)
        logfmt = config.get(
            cli_log_fmt, config.get(
                'log_fmt_console',
                config.get(
                    'log_fmt',
                    salt.config._DFLT_LOG_FMT_CONSOLE
                )
            )
        )

        cli_log_datefmt = 'cli_salt_cloud_log_datefmt'
        if cli_log_datefmt in config and not config.get(cli_log_datefmt):
            # Remove it from config so it inherits from log_datefmt_console
            config.pop(cli_log_datefmt)

        if config.get('log_datefmt_console', None) is None:
            # Remove it from config so it inherits from log_datefmt
            config.pop('log_datefmt_console', None)

        datefmt = config.get(
            cli_log_datefmt,
            config.get(
                'log_datefmt_console',
                config.get(
                    'log_datefmt',
                    '%Y-%m-%d %H:%M:%S'
                )
            )
        )

        salt.log.setup_console_logger(
            config['log_level'], log_format=logfmt, date_format=datefmt
        )

        # Now the log file logging
        if 'log_level_logfile' in config and not \
                config.get('log_level_logfile'):
            # Remove it from config so it inherits from log_level
            config.pop('log_level_logfile')

        loglevel = config.get(
            'log_level_logfile',
            # From the console setting
            config['log_level']
        )

        cli_log_path = 'cli_salt_cloud_log_file'
        if cli_log_path in config and not config.get(cli_log_path):
            # Remove it from config so it inherits from log_level_logfile
            config.pop(cli_log_path)

        if 'log_level_logfile' in config and not \
                config.get('log_level_logfile'):
            # Remove it from config so it inherits from log_file
            config.pop('log_level_logfile')

        logfile = config.get(
            # First from the config cli setting
            cli_log_path,
            config.get(
                # From the config setting
                'log_level_logfile'
                # From the default setting
                '/var/log/salt/cloud'
            )
        )

        cli_log_file_fmt = 'cli_salt_cloud_log_file_fmt'
        if cli_log_file_fmt in config and not config.get(cli_log_file_fmt):
            # Remove it from config so it inherits from log_fmt_logfile
            config.pop(cli_log_file_fmt)

        if config.get('log_fmt_logfile', None) is None:
            # Remove it from config so it inherits from log_fmt_console
            config.pop('log_fmt_logfile', None)

        log_file_fmt = config.get(
            cli_log_file_fmt,
            config.get(
                'cli_salt_cloud_log_fmt',
                config.get(
                    'log_fmt_logfile',
                    config.get(
                        'log_fmt_console',
                        config.get(
                            'log_fmt',
                            salt.config._DFLT_LOG_FMT_CONSOLE
                        )
                    )
                )
            )
        )

        cli_log_file_datefmt = 'cli_salt_cloud_log_file_datefmt'
        if cli_log_file_datefmt in config and not \
                config.get(cli_log_file_datefmt):
            # Remove it from config so it inherits from log_datefmt_logfile
            config.pop(cli_log_file_datefmt)

        if config.get('log_datefmt_logfile', None) is None:
            # Remove it from config so it inherits from log_datefmt_console
            config.pop('log_datefmt_logfile', None)

        if config.get('log_datefmt_console', None) is None:
            # Remove it from config so it inherits from log_datefmt
            config.pop('log_datefmt_console', None)

        log_file_datefmt = config.get(
            cli_log_file_datefmt,
            config.get(
                'cli_salt_cloud_log_datefmt',
                config.get(
                    'log_datefmt_logfile',
                    config.get(
                        'log_datefmt_console',
                        config.get(
                            'log_datefmt',
                            '%Y-%m-%d %H:%M:%S'
                        )
                    )
                )
            )
        )

        salt.log.setup_logfile_logger(
            logfile,
            loglevel,
            log_format=log_file_fmt,
            date_format=log_file_datefmt
        )

        # Now setup any granular logging levels
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
        config = self.__load_saltcloud_config().copy()

        profile = config['profiles'].get(self.saltcloud_profile_name, None)
        if profile is None:
            msg = 'The profile {0!r} does not exist.'.format(
                self.saltcloud_profile_name
            )
            log.error(msg)
            reactor.callLater(0, self.stop_instance)
            raise LatentBuildSlaveFailedToSubstantiate(
                self.saltcloud_profile_name, msg
            )

        minion_conf = saltcloud.config.get_config_value(
            'minion', profile, config, default={}
        )

        # Setup the required slave grains to be used by the minion
        if not minion_conf.get('master', None):
            import urllib2
            attempts = 5
            while attempts > 0:
                try:
                    request = urllib2.urlopen('http://v4.ident.me/')
                    public_ip = request.read()
                    minion_conf['master'] = public_ip
                    log.info(
                        'Found local public IP address, {0},  to be '
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
                log.warning(msg)
                reactor.callLater(0, self.stop_instance)
                raise LatentBuildSlaveFailedToSubstantiate(
                    self.saltcloud_profile_name, msg
                )

            # Set the buildbot slave name and password as a grain
            if 'grains' not in minion_conf:
                minion_conf['grains'] = {}
            if 'buildbot' not in minion_conf['grains']:
                minion_conf['grains']['buildbot'] = {}
            minion_conf['grains']['buildbot']['slavename'] = self.slavename
            # Set the buildbot password as a grain
            minion_conf['grains']['buildbot']['password'] = self.password

            # Remove settings that should be set at runtime
            minion_conf.pop('conf_file', None)

            # Update the virtual machines minion configuration
            config['profiles'][self.saltcloud_vm_name]['minion'] = minion_conf

        mapper = saltcloud.cloud.Map(config)
        try:
            ret = mapper.run_profile(
                self.saltcloud_profile_name, [self.saltcloud_vm_name]
            )
            if not ret:
                msg = 'Failed to start {0} for slave {1}'.format(
                    self.saltcloud_vm_name,
                    self.slavename
                )
                log.error(msg)
                reactor.callLater(0, self.stop_instance)
                raise LatentBuildSlaveFailedToSubstantiate(
                    self.saltcloud_vm_name, msg
                )

            if 'Errors' in ret[self.saltcloud_vm_name]:
                msg = (
                    'There were errors while trying to start salt-cloud VM '
                    '{0} for slave {1}: {2}'.format(
                        self.saltcloud_vm_name,
                        self.slavename,
                        ret[self.saltcloud_vm_name]['Errors']
                    )
                )
                log.error(msg)
                reactor.callLater(0, self.stop_instance)
                raise LatentBuildSlaveFailedToSubstantiate(
                    self.saltcloud_vm_name, msg
                )

            try:
                if 'Errors' in ret[self.saltcloud_vm_name][self.saltcloud_vm_name]:
                    msg = (
                        'There were errors while trying to start salt-cloud VM '
                        '{0} for slave {1}: {2}'.format(
                            self.saltcloud_vm_name,
                            self.slavename,
                            ret[self.saltcloud_vm_name][self.saltcloud_vm_name]['Errors']
                        )
                    )
                log.error(msg)
                reactor.callLater(0, self.stop_instance)
                raise LatentBuildSlaveFailedToSubstantiate(
                    self.saltcloud_vm_name, msg
                )
            except KeyError:
                pass

            log.info(
                'salt-cloud started VM {0} for slave {1}. '
                'Details:\n{2}'.format(
                    self.saltcloud_vm_name,
                    self.slavename,
                    salt.output.out_format(
                        ret[self.saltcloud_vm_name], 'pprint', config
                    )
                )
            )
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
            reactor.callLater(0, self.stop_instance)
            raise LatentBuildSlaveFailedToSubstantiate(
                self.saltcloud_vm_name, msg
            )

        # Let the minion connect back
        time.sleep(2)

        try:
            log.info('Running \'state.highstate\' on the minion')
            client = salt.client.LocalClient(c_path=self.saltcloud_master_config)
        except Exception as err:
            log.error(
                'Failed to instantiate the salt local client: {0}\n'.format(
                    err
                ),
                exc_info=True
            )

        try:
            attempts = 11
            while True:
                log.error(
                    'Publishing \'state.highstate\' job to {0}. '
                    'Attempts remaining {1}'.format(
                        self.saltcloud_vm_name,
                        attempts - 1
                    )
                )
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
                            reactor.callLater(0, self.stop_instance)
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
                        log.error(msg)
                        reactor.callLater(0, self.stop_instance)
                        raise LatentBuildSlaveFailedToSubstantiate(
                            self.saltcloud_vm_name, msg
                        )
                    continue

            log.info('Published job information: {0}'.format(job))

            # Let the job start
            time.sleep(6)

            attempts = 11
            job_running = False
            while True:
                log.info(
                    'Checking if \'state.highstate\' is running on '
                    '{0}. Attempts remaining {1}'.format(
                        self.saltcloud_vm_name,
                        attempts - 1
                    )
                )
                try:
                    running = client.cmd(
                        [self.saltcloud_vm_name],
                        'saltutil.running',
                        expr_form='list'
                    )
                    log.info('Running on the minion: {0}'.format(running))
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
                        reactor.callLater(0, self.stop_instance)
                        raise LatentBuildSlaveFailedToSubstantiate(
                            self.saltcloud_vm_name, msg
                        )
                    continue

                if not running or (running and not job_running and not
                                   running.get(self.saltcloud_vm_name, [])):
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
                        raise LatentBuildSlaveFailedToSubstantiate(
                            self.saltcloud_vm_name, msg
                        )
                    continue

                if job_running is False:
                    job_running = True

                # Reset failed attempts
                if attempts < 11:
                    log.info('Reseting failed attempts')
                    attempts = 11

                log.info(
                    'Job is still running on {0}: {1}'.format(
                        self.saltcloud_vm_name, running
                    )
                )
                if not [job_details for job_details in
                        running.get(self.saltcloud_vm_name, [])
                        if job_details and job_details['jid'] == job]:
                    # Job is no longer running
                    log.info(
                        '\'state.highstate\' has completed on {0}'.format(
                            self.saltcloud_vm_name
                        )
                    )
                    break
                time.sleep(5)

            log.info(
                'state.highstate has apparently completed in {0}'.format(
                    self.saltcloud_vm_name
                )
            )

            # Let the minion settle
            time.sleep(2)
            log.info(
                'Getting \'state.highstate\' job information from {0}'.format(
                    self.saltcloud_vm_name
                )
            )

            highstate = client.get_full_returns(
                job, [self.saltcloud_vm_name], timeout=5
            )
            try:
                log.info(
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
            except Exception:
                log.info(
                    'Output of running \'state.highstate\' on the {0} '
                    'minion({1}):\n{2}'.format(
                        self.slavename,
                        self.saltcloud_vm_name,
                        salt.output.out_format(
                            highstate, 'pprint', config
                        )
                    )
                )

            if not highstate or 'Error' in highstate:
                msg = (
                    'Returned empty or error running state.highstate on '
                    '{0} for slave {1}: {2}'.format(
                        self.saltcloud_vm_name,
                        self.slavename,
                        highstate
                    )
                )
                log.error(msg)
                reactor.callLater(0, self.stop_instance)
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
                log.error(msg)
                reactor.callLater(0, self.stop_instance)
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
                    reactor.callLater(0, self.stop_instance)
                    raise LatentBuildSlaveFailedToSubstantiate(
                        self.saltcloud_vm_name, msg
                    )
            log.info(
                'state.highstate completed without any issues on {0}'.format(
                    self.saltcloud_vm_name
                )
            )
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
            reactor.callLater(0, self.stop_instance)
            raise LatentBuildSlaveFailedToSubstantiate(
                self.saltcloud_vm_name, msg
            )

    def stop_instance(self, fast=False):
        # responsible for shutting down instance.
        log.info(
            'Shutting down VM {0} for slave {1}'.format(
                self.saltcloud_vm_name,
                self.slavename
            )
        )
        return threads.deferToThread(self.__stop_instance)

    def __stop_instance(self):
        config = self.__load_saltcloud_config()
        mapper = saltcloud.cloud.Map(config)
        try:
            ret = mapper.destroy([self.saltcloud_vm_name])
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
        finally:
            reactor.callLater(
                5, self.botmaster.maybeStartBuildsForSlave, self.name
            )
