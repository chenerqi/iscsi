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
""" KS SSH Client.

.. module: client

:Description: This is the KS Client that talks to iSCSI server.
It provides the ability to provision KS volumes.
"""
import os
import threading
import time

# Python 3+ override
try:
    basestring
    python3 = False
except NameError:
    basestring = str
    python3 = True

from iscsiclient import exceptions, ssh

class KSClient(threading.Thread):

    PORT = 3260
    
    mutex = threading.Lock()

    def TARGET_HEADER_FORMAT(self, target_iqn):
        return '''<target %s>
''' % target_iqn

    def BS_FORMAT(self, bs, lun):
        return '''  <backing-store %s>
    lun %d
  </backing-store>
''' % (bs, lun)

    def TARGET_FOOTER_FORMAT(self):
        return '''</target>

'''
    def __init__(self, ip, username, password, debug=False, secure=False, timeout=None,
                 suppress_ssl_warnings=False):
        self.ip = ip.split(',')
        self.channels = self._get_channels(username, password)

    def _get_channels(self, username, password):
        channels = []
        for ip in self.ip:
            channel = dict()
            channel['ip'] = ip
            channel['ssh'] = ssh.SSHClient(ip, username, password)
            channel['iscsi_target'] = self._get_iscsi_target(ip)
           
            channels.append(channel)
        return channels
 
    def _get_iscsi_target(self, ip):
        target_portal = '%s:%d' % (ip, self.PORT)

        cmd_gettargets = 'iscsiadm -m discovery -t sendtargets -p %s' % target_portal
        output = os.popen(cmd_gettargets).read()
        '''
        iscsiadm -m discovery -t sendtargets -p 192.168.47.128:3260
        192.168.47.128:3260,1 iqn.2018-03.com.kingstack:iscsi
        '''
        if python3:
            output = output.decode().split()
        else:
            output = output.split()

        if output[1].find('iqn') == -1:
            raise exceptions.ClientException('Target discovery failed')
        iscsi_target = dict()
        iscsi_target['target_portal'] = output[0].split(',')[0]
        iscsi_target['target_id'] = int(output[0].split(',')[1])
        iscsi_target['target_iqn'] = output[1]
        return iscsi_target

    def _run(self, cmd, ssh=None, wait_result=True):
        try:
			self.mutex.acquire()
			if ssh is None:
				ssh = self.channels[0]['ssh']
			if ssh is None:
				raise exceptions.SSHException('SSH is not initialized.')
			else:
				ssh.open()
				return ssh.run(cmd, wait_result)
        except Exception as e:
            raise e
        finally:
            self.mutex.release()

    def _create_rbd_image(self, cmd_createimage):
        output = self._run(cmd_createimage)
        if output != []:
            output = '\r\n'.join(output)
            if output.find('exist') != -1:
                reason = 'image has exist'
            elif output.find('error opening pool') != -1 and\
                    output.find('No such file or directory'):
                reason = 'pool dosen\'t exist'
            else:
                reason = output

            raise exceptions.ClientException('rbd image create failed - '
                                             + reason)

    def _delete_rbd_image(self, pool, name):
        cmd_deleteimage = ['rbd rm %s/%s' % (pool, name)]
        self._run(cmd_deleteimage, wait_result=False)

    def _resize_rbd_image(self, pool, name, new_sizeGB):
        cmd_extendimage = ['rbd resize %s/%s -s %dG' %
                           (pool, name, new_sizeGB)]
        output = self._run(cmd_extendimage)

        if output and output[0].find('done') == -1:
            message = 'Extend volume %s failed' % name
            raise exceptions.ClientException(message)

    def _create_lun(self, pool, name, lun):
        """if fails during creation,
        return the success channels for rollback
        """
        error = 0
        success_channels = []
        for channel in self.channels:
            cmd = 'tgtadm --lld iscsi --op new --mode logicalunit -t %d '
            cmd += '--lun %d --backing-store %s/%s --bstype rbd'
            cmd_createlun = [cmd % (channel['iscsi_target']['target_id'],
                                    lun, pool, name)]
            output = self._run(cmd_createlun, ssh=channel['ssh'])
            if output != []:
                error = -1
                return error, success_channels
            success_channels.append(channel)
        return error, None

    def _delete_lun(self, lun, channels=None):
        if channels is None:
            channels = self.channels
        for channel in channels:
            cmd_deletelun = ['tgtadm --lld iscsi --op delete --mode '
                             'logicalunit -t %d --lun %d' %
                             (channel['iscsi_target']['target_id'], lun)]
            self._run(cmd_deletelun, ssh=channel['ssh'], wait_result=False)

    def _disable_lun(self, lun):
        disabled_channels = []
        for channel in self.channels:
            cmd_setlun = ['tgtadm -m logicalunit -o update --tid %d '
                          '--lun %d -a 0' %
                          (channel['iscsi_target']['target_id'], lun)]
            output = self._run(cmd_setlun, ssh=channel['ssh'])
            if output != []:
                message = 'Disable LUN %d failed' % lun
                if len(disabled_channels) > 0:
                    # enable the disabled channels for rollback
                    try:
                        self._enable_lun(lun, disabled_channels)
                    except exceptions.ClientException as e:
                        message = 'Rollback failed - ' + e.message
                raise exceptions.ClientException(message)
            disabled_channels.append(channel)

    def _enable_lun(self, lun, channels=None):
        if channels is None:
            channels = self.channels

        error = 0
        failed_channels = []
        ip = []
        for channel in channels:
            cmd_setlun = ['tgtadm -m logicalunit -o update --tid %d'
                          ' --lun %d -a 1' %
                          (channel['iscsi_target']['target_id'], lun)]
            output = self._run(cmd_setlun, ssh=channel['ssh'])
            if output != []:
                error = -1
                failed_channels.append(channel)

        if error == -1:
            error = 0
            for channel in failed_channels:
                output = self._run(cmd_setlun, ssh=channel['ssh'])
                if output != []:
                    error = -1
                    ip.append(channel['ip'])

        if error == -1:
            message = 'Enable LUN %s failed\r\nPlase manually enable for IP:' \
                      ' %s' % (lun, ' '.join(ip))
            raise exceptions.ClientException(message)

    def _update_lun(self, pool, name, lun):
        # if fails during creation, return the success channels for rollback
        error = 0
        for channel in self.channels:
            cmd = 'tgtadm --lld iscsi --op update --mode logicalunit -t %d '
            cmd += '--lun %d --backing-store %s/%s'
            cmd_updatelun = [cmd % (channel['iscsi_target']['target_id'], lun,
                                    pool, name)]
            output = self._run(cmd_updatelun, ssh=channel['ssh'])
            if output != []:
                error = -1
        return error

    def _add_lun_to_conf_file(self, pool, name, lun):
        lun_file = 'lun%d.conf' % lun
        local_conf_file = '/tmp/'  + lun_file
        bs = dict()
        bs['backing-store'] = '%s/%s' % (pool, name)
        bs['lun'] = lun
        for channel in self.channels:
            with open(local_conf_file, 'w') as f:
                f.truncate()
                f.write(self.TARGET_HEADER_FORMAT(channel['iscsi_target']
                                                  ['target_iqn']))
                f.write(self.BS_FORMAT(bs['backing-store'], bs['lun']))
                f.write(self.TARGET_FOOTER_FORMAT())

                remote_conf_file = '/etc/tgt/' + \
				                   channel['iscsi_target']['target_iqn'] + lun_file

            channel['ssh'].put_remote_file(local_conf_file, remote_conf_file)
        os.remove(local_conf_file)

    def _delete_lun_from_conf_file(self, lun):
        lun_file = 'lun%d.conf' % lun

        for channel in self.channels:
            remote_conf_file = '/etc/tgt/' + \
			                   channel['iscsi_target']['target_iqn'] + lun_file

            remote_bak_file = '/etc/tgt/' + \
			                  channel['iscsi_target']['target_iqn'] + lun_file + '.bak'

            cmd_bak_remote_conf_file = ['\\cp %s %s -f' % (remote_conf_file,
                                                           remote_bak_file)]
            self._run(cmd_bak_remote_conf_file, ssh=channel['ssh'])

            cmd_rm_remote_conf_file = ['rm %s -rf' % remote_conf_file]
            self._run(cmd_rm_remote_conf_file, ssh=channel['ssh'])

    def create_lun(self, pool, name, lun):
        """get free lun, if no vaild free lun,
        delete rbd image for rollback then raise exception
        """

        # lun = self._get_free_lun()
        if lun < 1:
            self._delete_rbd_image(pool, name)
            raise exceptions.ClientException("Invalid LUN %d" % lun)

        # create lun in memory
        error, success_channels = self._create_lun(pool, name, lun)
        if error == -1:
            if len(success_channels) > 0:
                self._delete_lun(lun, success_channels)
            self._delete_rbd_image(pool, name)
            raise exceptions.ClientException('LUN create failed %d' % lun)

        # create lun in config file
        self._add_lun_to_conf_file(pool, name, lun)

        return {'id': lun}

    def create_volume(self, volume, pool, lun):
        """Create a new volume.

        :param name: the volume
        :type name: object
        :param pool in volume: rbd pool
        :type size: parse from host
        :param name in volume: rbd image name
        :type size: str
        :param in volume size: rbd image size
        :type size: int
        :returns: new volume lun id
        """

        name = volume.name
        if name is None or name == '':
            raise exceptions.ClientException("Invalid volume name %s" % name)
        size = volume.size
        if size < 1:
            raise exceptions.ClientException("Invalid volume size: %d" % size)
        # pool = volume.host[volume.host.find('#') + 1:]

        cmd_createimage = ['rbd create %s/%s -s %dG' % (pool, name, size)]
        self._create_rbd_image(cmd_createimage)
        lun = self.create_lun(pool, name, lun)
        return lun

    def create_volume_from_snapshot(self, volume, snapshot, lun, pool,
                                    flatten=False, is_temp=False):
        """Create a new volume from snapshot.
         :param volume: the volume
         :type volume: object
         :param snapshot: the snapshot
         :type snapshot: object
         :param name in sanpshot: name of the snapshot
         :type name: str
         """

        name = volume.name
        if name is None or name == '':
            raise exceptions.ClientException("Invalid volume name %s" % name)
        if is_temp:
            snap_volume_name = snapshot["volume_name"]
            snap_name = snapshot["name"]
        else:
            snap_volume_name = snapshot.volume_name
            snap_name = snapshot.name
        # pool = snapshot.volume.host[snapshot.volume.host.find('#') + 1:]

        self._check_snapshot_exists(pool, snap_volume_name, snap_name)

        cmd_protect_snap = ['rbd snap protect %s/%s@%s'
                            % (pool, snap_volume_name, snap_name)]
        self._run(cmd_protect_snap)
        try:
            cmd_createimage = ['rbd clone %s/%s@%s %s/%s'
                               % (pool, snap_volume_name,
                                  snap_name, pool, name)]
            self._create_rbd_image(cmd_createimage)
            lun = self.create_lun(pool, name, lun)
        except Exception as e:
            cmd_unprotect_snap = ['rbd snap unprotect %s/%s@%s'
                                  % (pool, snap_volume_name, snap_name)]
            self._run(cmd_unprotect_snap)
            raise e

        if flatten:
            cmd_flattenimage = ['rbd flatten %s/%s' % (pool, name)]
            self._run(cmd_flattenimage)

        return lun

    def delete_volume(self, volume, pool):
        """Delete a volume.

        :param name: the name of the volume
        :type name: str
        """

        name = volume.name
        if name is None or name == '':
            raise exceptions.ClientException("Invalid volume name %s" % name)
        lun = self.get_lun_id(volume)
        if lun < 1:
            return
        # pool = volume.host[volume.host.find('#') + 1:]

        cmd_listimage = ['rbd info %s/%s' % (pool, name)]
        output = self._run(cmd_listimage)
        if '\r\n'.join(output).find('No such file or directory') != -1:
            raise exceptions.ClientException("Volume dosen't exist %s/%s"
                                             % (pool, name))

        cmd_listsnap = ['rbd snap ls %s/%s' % (pool, name)]
        output = self._run(cmd_listsnap)
        if output != []:
            raise exceptions.ClientException('Volume has snapshot,'
                                             ' please remove snapshot first')
        self._delete_lun(lun)
        self._delete_rbd_image(pool, name)

        self._delete_lun_from_conf_file(lun)

    def extend_volume(self, volume, new_size, pool):
        """Extend an existing volume to new size by GB.

        :param volume: the object of the volume
        :type name: object
        :param amount: the new size in GB
        :type amount: int
        """

        if volume.size > new_size:
            raise exceptions.ClientException('Volume can only be extend')

        name = volume.name
        if name is None or name == '':
            raise exceptions.ClientException("Invalid volume name %s" % name)
        lun = self.get_lun_id(volume)
        if lun < 1:
            raise exceptions.ClientException("Invalid volume lun id: %d" % lun)
        # pool = volume.host[volume.host.find('#') + 1:]

        # disable lun
        self._disable_lun(lun)

        # resize rbd image
        self._resize_rbd_image(pool, name, new_size)

        # update lun
        error = self._update_lun(pool, name, lun)
        if error == -1:
            raise exceptions.ClientException('Update volume failed,'
                                             ' needs operate manually')
        # enable lun
        self._enable_lun(lun)

    def _check_snapshot_exists(self, pool, volume_name, snap_name):
        cmd_listsnap = ['rbd snap ls %s/%s' % (pool, volume_name)]
        output = self._run(cmd_listsnap)
        output = '\r\n'.join(output)
        if output.find(snap_name) == -1:
            message = 'snapshot %s/%s@%s dosen\'t exist' \
                      % (pool, volume_name, snap_name)
            raise exceptions.ClientException(message)

    def create_snapshot(self, snapshot, pool, is_temp=False):
        """Create a snapshot of an existing volume.
        :param snapshot: the snapshot
        :type snapshot: object
        :param volume_name in sanpshot: name of the volume
        :type name: str
        :param name in sanpshot: name of the snapshot
        :type name: str
        """

        if is_temp:
            volume_name = snapshot["volume_name"]
            name = snapshot["name"]
        else:
            volume_name = snapshot.volume_name
            name = snapshot.name
        # pool = snapshot.volume.host[snapshot.volume.host.find('#') + 1:]

        cmd_listimage = ['rbd info %s/%s' % (pool, volume_name)]
        output = self._run(cmd_listimage)
        if '\r\n'.join(output).find('No such file or directory') != -1:
            raise exceptions.ClientException("Volume dosen't exist %s/%s"
                                             % (pool, volume_name))

        cmd_createsnap = ['rbd snap create %s/%s@%s'
                          % (pool, volume_name, name)]
        output = self._run(cmd_createsnap)
        if output != []:
            output = '\r\n'.join(output)
            if output.find('File exists') != -1:
                reason = 'snapshot has exist'
            else:
                reason = output
            raise exceptions.ClientException('snapshot create failed - '
                                             + reason)

    def delete_snapshot(self, snapshot, pool, is_temp=True):
        """Delete a snapshot of an existing volume.
        :param snapshot: the snapshot
        :type snapshot: object
        :param volume_name in sanpshot: name of the volume
        :type name: str
        :param name in sanpshot: name of the snapshot
        :type name: str
        """

        if is_temp:
            volume_name = snapshot["volume_name"]
            snap_name = snapshot["name"]
        else:
            volume_name = snapshot.volume_name
            snap_name = snapshot.name
        # pool = snapshot.volume.host[snapshot.volume.host.find('#') + 1:]

        self._check_snapshot_exists(pool, volume_name, snap_name)
        cmd_unprotect_snap = ['rbd snap unprotect %s/%s@%s'
                              % (pool, volume_name, snap_name)]
        output = self._run(cmd_unprotect_snap)
        if '\r\n'.join(output).find('cannot unprotect') != -1:
            raise exceptions.ClientException('snapshot unprotect failed %s'
                                             % output)

        cmd_delete_snap = ['rbd snap rm %s/%s@%s'
                           % (pool, volume_name, snap_name)]
        output = self._run(cmd_delete_snap)
        if output != []:
            output = '\r\n'.join(output)
            if output.find('100% complete') == -1:
                raise exceptions.ClientException('snapshot delete failed %s'
                                                 % output)

    def rollback_snapshot(self, volume, snapshot, pool):
        """Rollback a snapshot of an existing volume.
        :param volume: the volume
        :type volume: object
        :param snapshot: the snapshot
        :type snapshot: object
        :param name in sanpshot: name of the snapshot
        :type name: str
        """

        name = volume.name
        if name is None or name == '':
            raise exceptions.ClientException("Invalid volume name %s" % name)
        lun = self.get_lun_id(volume)
        snap_name = snapshot.name
        # pool = snapshot.volume.host[snapshot.volume.host.find('#') + 1:]

        self._check_snapshot_exists(pool, name, snap_name)

        # disable lun
        self._disable_lun(lun)

        # rollback snapshot
        cmd_rollback_snap = ['rbd snap rollback %s/%s@%s'
                             % (pool, name, snap_name)]
        self._run(cmd_rollback_snap, wait_result=False)

        # update lun
        error = self._update_lun(pool, name, lun)
        if error == -1:
            raise exceptions.ClientException('Update volume failed,'
                                             ' needs operate manually')

        # enable lun
        self._enable_lun(lun)

    def initialize_connection(self, volume):
        """Get iSCSI targets infomation"""
        lun = self.get_lun_id(volume)
        if lun < 1:
            raise exceptions.ClientException('Invaild lun in volume')
        connection = dict()
        connection['driver_volume_type'] = 'iscsi'
        data = dict()
        data['target_luns'] = []
        data['target_iqns'] = []
        data['target_portals'] = []
        for channel in self.channels:
            target = channel['iscsi_target']
            cmd_statlun = ['tgtadm -m logicalunit -o stat -t %d -l %d' %
                           (target['target_id'], lun)]
            output = self._run(cmd_statlun, ssh=channel['ssh'])
            if '\r\n'.join(output).find('can\'t find the logical unit') != -1:
                message = 'LUN %d doesn\'t exist in ip %s target %d'\
                          % (lun, channel['ssh'].ip, target['target_id'])
                raise exceptions.ClientException(message)
            data['target_luns'].append(lun)
            data['target_iqns'].append(target['target_iqn'])
            data['target_portals'].append(target['target_portal'])

        data['target_discovered'] = True
        first_target = self.channels[0]['iscsi_target']
        data['target_iqn'] = first_target['target_iqn']
        data['target_portal'] = first_target['target_portal']
        data['target_lun'] = lun
        data['volume_id'] = volume.name
        connection['data'] = data

        return connection

    def terminate_connection(self, volume, connector=None):
        return

    def _size_to_GB(self, size):
        if size.find('G') != -1:
            return int(size.strip('G'))
        elif size.find('M') != -1:
            size = float(int(size.strip('M')) / 1024)
        elif size.find('k') != -1:
            size = float(int(size.strip('k')) / 1024 / 1024)
        else:
            size = float(int(size) / 1024 / 1024)
        if size > 0:
            return int(size)
        else:
            return round(size, 3)

    def get_volume_backend_info(self):
        """NAME     ID     USED     %USED     MAX AVAIL     OBJECTS
        rbd      1        17         0         7983M           4
       """

        cmd_df = ['ceph df']
        output = self._run(cmd_df)
        backend_infos = list()
        for i, line in enumerate(output):
            if line.find('NAME') != -1 and \
               line.find('ID') != -1 and \
               line.find('USED') != -1 and \
               line.find('MAX AVAIL') != -1 and \
               line.find('OBJECTS') != -1:
                output = output[i + 1:]
                break
        for line in output:
            p = line.split()
            backend_info = dict()
            backend_info['id'] = p[0]
            backend_info['name'] = p[0]
            backend_info['sizeSubscribed'] = self._size_to_GB(p[2])
            backend_info['sizeFree'] = self._size_to_GB(p[4])
            backend_info['sizeTotal'] = int(backend_info['sizeSubscribed'] +
                                            backend_info['sizeFree'])
            backend_infos.append(backend_info)
        if len(backend_infos) == 0:
            raise exceptions.ClientException('No valid pool exists')
        return backend_infos

    def get_lun_id(self, volume):
        lun_id = 0
        if volume.provider_location:
            lun_id = int(volume.provider_location.split("-")[1])
        return lun_id
