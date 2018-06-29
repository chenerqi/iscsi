# (c) Copyright 2018-2020 ceph iscsi
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Exceptions for the ssh

.. module: sshexceptions

:Author: ceph
:Description: This contains the ssh exceptions
"""

import logging

# Python 3+ override
try:
    basestring
except NameError:
    basestring = str

LOG = logging.getLogger(__name__)


class SSHException(Exception):
    """This is the basis for the SSH Exceptions."""

    code = 500
    message = "An unknown exception occurred."

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if 'code' not in self.kwargs:
            try:
                self.kwargs['code'] = self.code
            except AttributeError:
                pass

        if not message:
            try:
                message = self.message % kwargs

            except Exception:
                # kwargs doesn't match a variable in the message
                # log the issue and the kwargs
                LOG.exception('Exception in string format operation')
                for name, value in list(kwargs.items()):
                    LOG.error("%s: %s" % (name, value))
                # at least get the core message out if something happened
                message = self.message

        self.message = message
        super(SSHException, self).__init__(message)


class SSHInjectionThreat(SSHException):
    message = "SSH command injection detected: %(command)s"


class ProcessExecutionError(Exception):
    def __init__(self, stdout=None, stderr=None, exit_code=None, cmd=None,
                 description=None):
        self.exit_code = exit_code
        self.stderr = stderr
        self.stdout = stdout
        self.cmd = cmd
        self.description = description

        if description is None:
            description = "Unexpected error while running command."
        if exit_code is None:
            exit_code = '-'
        message = ("%s\nCommand: %s\nExit code: %s\nStdout: %r\nStderr: %r"
                   % (description, cmd, exit_code, stdout, stderr))
        super(ProcessExecutionError, self).__init__(message)


class ClientException(Exception):
    """The base exception class for all exceptions this library raises.

    :param message: error message
    :type message: str

    """
    message = "An unknown exception occurred."

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if not message:
            try:
                message = self.message % kwargs

            except Exception:
                # kwargs doesn't match a variable in the message
                # log the issue and the kwargs
                LOG.exception('Exception in string format operation')
                for name, value in list(kwargs.items()):
                    LOG.error("%s: %s" % (name, value))
                # at least get the core message out if something happened
                message = self.message

        self.message = message
        super(ClientException, self).__init__(message)
