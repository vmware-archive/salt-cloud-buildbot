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
import random
import logging

# Import twisted libs
from twisted.internet import defer, reactor, threads

# Import salt & salt-cloud libs
import salt.log
import salt.config
import saltcloud.cloud
import saltcloud.config

# Import buildbot libs
from buildbot.buildslave import AbstractLatentBuildSlave
from buildbot import interfaces


# Setup the salt temporary logging
salt.log.setup_temp_logger()

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
        self.saltcloud_vm_config = saltcloud_vm_config or '/etc/salt/cloud.profiles'
        self.saltcloud_master_config = saltcloud_master_config or '/etc/salt/master'
        self.saltcloud_profile_name = saltcloud_profile_name


    def __load_saltcloud_config(self):
        if self._saltcloud_config is not None:
            return self._saltcloud_config

        # Read/Parse salt-cloud configurations
        # salt master configuration
        master_config = salt.config.master_config(self.saltcloud_master_config)

        # salt-cloud config
        cloud_config = saltcloud.config.cloud_config(
            self.saltcloud_config
        )

        # profiles configuration
        profiles_config = saltcloud.config.vm_profiles_config(
            self.saltcloud_vm_config
        )

        config = master_config.copy()
        config.update(cloud_config)
        config['vm'] = profiles_config
        # Update with some parsers cli defaults
        config.update({
            'map': '',
            'deploy': True,
            'parallel': False,
            'keep_tmp': False
        })

        # The profile we wish to run
        config['profile'] = self.saltcloud_profile_name

        # The machine name
        config['names'] = [self.saltcloud_vm_name]
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
    @defer.inlineCallbacks
    def start_instance(self, build):
        # responsible for starting instance that will try to connect with this
        # master. Should return deferred with either True (instance started)
        # or False (instance not started, so don't run a build here). Problems
        # should use an errback.

        config = self.__load_saltcloud_config()

        # Setup the required slave grains to be used by the minion
        if 'master' not in config['minion']:
            import urllib2
            public_ip = yield urllib2.urlopen('http://v4.ident.me/').read()
            config['minion']['master'] = public_ip
        config['minion']['grains']['buildbot']['slavename'] = self.slavename
        config['minion']['grains']['buildbot']['password'] = self.password

        mapper = saltcloud.cloud.Map(config)
        try:
            ret = yield mapper.run_profile()
            log.info(
                'salt-cloud instance({0}) started. Details: {1}'.format(
                    self.slavename,
                    salt.output.out_format(ret, 'pprint', config)
                )
            )
            defer.returnValue(True)
        except Exception, err:
            log.error('There was a profile error.', exc_info=True)
            defer.returnValue(False)
            #raise interfaces.LatentBuildSlaveFailedToSubstantiate(
            #    self.slavename, err
            #)

    @defer.inlineCallbacks
    def stop_instance(self, fast=False):
        # responsible for shutting down instance.
        config = self.__load_saltcloud_config()
        mapper = yield saltcloud.cloud.Map(config)
        try:
            ret = yield mapper.destroy(config['names'])
            log.info(
                'salt-cloud instance({0}) stopped. Details: {1}'.format(
                    self.slavename,
                    salt.output.out_format(ret, 'pprint', config)
                )
            )
            defer.returnValue(True)
        except Exception, err:
            log.debug(
                'There was an error destroying machines.', exc_info=True
            )
            defer.returnValue(False)
