# -*- coding: utf-8 -*-
'''
    saltcloud_buildbot.steps
    ~~~~~~~~~~~~~~~~~~~~~~~~

    Buildbot Steps

    :codeauthor: :email:`Pedro Algarvio (pedro@algarvio.me)`
    :copyright: © 2013 by the SaltStack Team, see AUTHORS for more details.
    :license: Apache 2.0, see LICENSE for more details.
'''

from buildbot.steps.shell import ShellCommand


class SaltCallCommand(ShellCommand):

    logfiles = {'minion.log': '/var/log/salt/minion'}

    def __init__(self, salt_call_args, **kwargs):
        command = []
        sudo_required = kwargs.pop('sudo_required', False)
        if sudo_required:
            command.append('sudo')
        command.append('salt-call')
        if salt_call_args:
            if isinstance(salt_call_args, basestring):
                salt_call_args = salt_call_args.split()
            command.extend(salt_call_args)
        kwargs['command'] = command
        ShellCommand.__init__(self, **kwargs)


class SaltStateCommand(SaltCallCommand):

    def __init__(self, state_name, salt_call_args=None, **kwargs):
        if isinstance(salt_call_args, basestring):
            salt_call_args = salt_call_args.split()
        sudo_required = kwargs.pop('sudo_required', False)
        command = []
        if sudo_required:
            command.append('sudo')
        command.append('salt-call')
        if salt_call_args:
            if isinstance(salt_call_args, basestring):
                salt_call_args = salt_call_args.split()
            command.extend(salt_call_args)
        command.extend(['state.single', state_name])
        kwargs['command'] = command
        ShellCommand.__init__(self, **kwargs)
