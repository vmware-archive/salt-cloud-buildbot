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
import os
import random

# Import twisted libs
from twisted.internet import defer, error, protocol, reactor, threads

# Import buildbot libs
from buildbot.buildslave import AbstractLatentBuildSlave
from buildbot import interfaces


class SaltCloudProcessProtocol(protocol.ProcessProtocol):
    outBuffer = ''
    errBuffer = ''

    def connectionMade(self):
        self.deferred = defer.Deferred()

    def outReceived(self, data):
        self.outBuffer += data

    def errReceived(self, data):
        self.errBuffer += data

    def processEnded(self, reason):
        if reason.check(error.ProcessDone):
            self.deferred.callback(self.outBuffer)
        else:
            self.deferred.errback(reason)


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

        self.saltcloud_vm_name = '{0}-buildbot-rnd{1:04d}'.format(
            self.name, random.randrange(0, 10001, 2)
        )
        self.saltcloud_config = saltcloud_config or '/etc/salt/cloud'
        self.saltcloud_vm_config = saltcloud_vm_config or '/etc/salt/cloud.profiles'
        self.saltcloud_master_config = saltcloud_master_config or '/etc/salt/master'
        self.saltcloud_profile_name = saltcloud_profile_name

    # AbstractLatentBuildSlave methods
    def start_instance(self, build):
        # responsible for starting instance that will try to connect with this
        # master. Should return deferred with either True (instance started)
        # or False (instance not started, so don't run a build here). Problems
        # should use an errback.
        protocol = SaltCloudProcessProtocol()

        return reactor.spawnProcess(
            protocol,
            'salt-cloud',
            args=[
                'salt-cloud',
                '-C', self.saltcloud_config,
                '-M', self.saltcloud_master_config,
                '-V', self.saltcloud_vm_config,
                '-p', self.saltcloud_profile_name,
                self.saltcloud_vm_name
            ],
            env=os.environ.copy(),
            #path,
            #uid,
            #gid,
            #usePTY,
            #childFDs
        ).deferred

    def stop_instance(self, fast=False):
        # responsible for shutting down instance.
        protocol = SaltCloudProcessProtocol()

        return reactor.spawnProcess(
            protocol,
            'salt-cloud',
            args=[
                'salt-cloud',
                '-C', self.saltcloud_config,
                '-M', self.saltcloud_master_config,
                '-V', self.saltcloud_vm_config,
                '-d', self.saltcloud_vm_name
            ],
            env=os.environ.copy(),
            #path,
            #uid,
            #gid,
            #usePTY,
            #childFDs
        ).deferred
