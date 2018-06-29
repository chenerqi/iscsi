# Copyright (c) 2018 ceph iscsi Corporation.
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
SSH Client

.. module: ssh

:Description: This is the SSH Client that is used to remote execute cmd
              where an existing REST API doesn't exist.
"""

import os
from oslo_log import log as logging
import paramiko
from random import randint

from cinder.volume.drivers.rbd_iscsi.iscsiclient import exceptions
from eventlet import greenthread

# Python 3+ override
try:
    basestring
    python3 = False
except NameError:
    basestring = str
    python3 = True

LOG = logging.getLogger(__name__)


class SSHClient(object):
    """This class is used to execute SSH commands."""

    def __init__(self, ip, username, password,
                 port=22, conn_timeout=None, privatekey=None,
                 **kwargs):
        self.ip = ip
        self.port = port
        self.username = username
        self.password = password
        self.conn_timeout = conn_timeout
        self.privatekey = privatekey

        self._create_ssh(**kwargs)

    def _create_ssh(self, **kwargs):
        try:
            ssh = paramiko.SSHClient()
            known_hosts_file = kwargs.get('known_hosts_file', None)
            if known_hosts_file is None:
                ssh.load_system_host_keys()
            else:
                # Make sure we can open the file for appending first.
                # This is needed to create the file when we run CI tests with
                # no existing key file.
                open(known_hosts_file, 'a').close()
                ssh.load_host_keys(known_hosts_file)
            missing_key_policy = kwargs.get('missing_key_policy', None)
            if missing_key_policy is None:
                missing_key_policy = paramiko.AutoAddPolicy()
            elif isinstance(missing_key_policy, basestring):
                # To make it configurable, allow string to be mapped to object.
                if missing_key_policy == paramiko.AutoAddPolicy().\
                        __class__.__name__:
                    missing_key_policy = paramiko.AutoAddPolicy()
                elif missing_key_policy == paramiko.RejectPolicy().\
                        __class__.__name__:
                    missing_key_policy = paramiko.RejectPolicy()
                elif missing_key_policy == paramiko.WarningPolicy().\
                        __class__.__name__:
                    missing_key_policy = paramiko.WarningPolicy()
                else:
                    raise exceptions.SSHException("Invalid missing_key_policy:"
                                                  " %s" % missing_key_policy)

            ssh.set_missing_host_key_policy(missing_key_policy)

            self.ssh = ssh
        except Exception as e:
            msg = "Error connecting via ssh: %s" % e
            LOG.error(msg)
            raise paramiko.SSHException(msg)

    def _connect(self, ssh):
        if self.password:
            ssh.connect(self.ip,
                        port=self.port,
                        username=self.username,
                        password=self.password,
                        timeout=self.conn_timeout)
        elif self.privatekey:
            pkfile = os.path.expanduser(self.san_privatekey)
            privatekey = paramiko.RSAKey.from_private_key_file(pkfile)
            ssh.connect(self.ip,
                        port=self.port,
                        username=self.login,
                        pkey=privatekey,
                        timeout=self.conn_timeout)
        else:
            msg = "Specify a password or private_key"
            raise exceptions.SSHException(msg)

    def open(self):
        """Opens a new SSH connection if the transport layer is missing.
        This can be called if an active SSH connection is open already.
        Create a new SSH connection if the transport layer is missing.
        """
        if self.ssh:
            transport_active = False
            transport = self.ssh.get_transport()
            if transport:
                transport_active = self.ssh.get_transport().is_active()
            if not transport_active:
                try:
                    self._connect(self.ssh)
                except Exception as e:
                    msg = "Error connecting via ssh: %s" % e
                    LOG.error(msg)
                    raise paramiko.SSHException(msg)

    def close(self):
        if self.ssh:
            self.ssh.close()

    def run(self, cmd, wait_result=True):
        """Runs a CLI command over SSH, without doing any result parsing."""
        LOG.debug("SSH CMD = %s " % cmd)
		
        (out, err) = self._run_ssh(cmd, wait_result)
        LOG.debug("SSH stdout = %s " % out)

        return out

    def _run_ssh(self, cmd_list, wait_result, attempts=2):
        self.check_ssh_injection(cmd_list)
        command = ' '. join(cmd_list)

        try:
            total_attempts = attempts
            while attempts > 0:
                attempts -= 1
                try:
                    return self._ssh_execute(command, wait_result)
                except Exception as e:
                    LOG.error(e)
                    if attempts > 0:
                        greenthread.sleep(randint(20, 500) / 100.0)
                    if not self.ssh.get_transport() or \
                            not self.ssh.get_transport().is_alive():
                        self._create_ssh()

            msg = ("SSH Command failed after '%(total_attempts)r' "
                   "attempts : '%(command)s'" %
                   {'total_attempts': total_attempts, 'command': command})
            LOG.error(msg)
            raise exceptions.SSHException(message=msg)
        except Exception:
            LOG.error("Error running ssh command: %s" % command)
            raise

    def _ssh_execute(self, cmd, wait_result, check_exit_code=True):
        LOG.debug('Running cmd (SSH): %s', cmd)
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        if wait_result:
            out, err = stdout.readlines(), stderr.readlines()
            LOG.debug("SSH stdout = %s " % out)
            LOG.debug("SSH err = %s " % err)
        else:
            out, err = None, None
        self.ssh.exec_command('exit')

        return (out, err)

    def check_ssh_injection(self, cmd_list):
        ssh_injection_pattern = ['`', '$', '|', '||', ';', '&', '&&',
                                 '>', '>>', '<']

        # Check whether injection attacks exist
        for arg in cmd_list:
            # Second, check whether danger character in command. So the shell
            # special operator must be a single argument.
            for c in ssh_injection_pattern:
                if arg == c:
                    continue

                result = arg.find(c)
                if not result == -1:
                    if result == 0 or not arg[result - 1] == '\\':
                        raise exceptions.SSHInjectionThreat(command=cmd_list)

    def get_remote_file(self, remote_file, local_file):
        if not self.ssh.get_transport() or \
                not self.ssh.get_transport().is_alive():
            self._create_ssh()
        sftp = self.ssh.open_sftp()
        sftp.get(remote_file, local_file)
        sftp.close()

    def put_remote_file(self, local_file, remote_file):
        if not self.ssh.get_transport() or \
                not self.ssh.get_transport().is_alive():
            self._create_ssh()
        sftp = self.ssh.open_sftp()
        sftp.put(local_file, remote_file)
        sftp.close()
