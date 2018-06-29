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
Drivers for EMC Unity array based on RESTful API.
"""

from oslo_config import cfg
from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _, _LI, _LE
from cinder.volume.drivers.rbd_iscsi.iscsiclient import client
from cinder.volume.drivers.san import san

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
VERSION = "1.0.0"

GiB = 1024 * 1024 * 1024

loc_opts = [
    cfg.StrOpt('storage_protocol',
               help='Storage protocol.'),
    cfg.StrOpt('storage_pool_name',
               default=None,
               deprecated_name='storage_pool_name',
               help='Comma-separated list of storage pool names to be used.'),
    cfg.BoolOpt('flatten_volume_from_snapshot',
                default=True,
                help='Flatten volumes created from snapshots to remove '
                     'dependency from volume to snapshot')]


CONF.register_opts(loc_opts)


class RBDISCSIHelper(object):

    def __init__(self, conf, driver):
        self.driver = driver
        self.configuration = conf
        self.configuration.append_config_values(loc_opts)
        self.configuration.append_config_values(san.san_opts)
        self.storage_protocol = conf.storage_protocol
        self.supported_storage_protocols = ('iSCSI', 'FC')
        if self.storage_protocol not in self.supported_storage_protocols:
            msg = _('storage_protocol %(invalid)s is not supported. '
                    'The valid one should be among %(valid)s.') % {
                'invalid': self.storage_protocol,
                'valid': self.supported_storage_protocols}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        self.active_storage_ip = self.configuration.san_ip
        self.storage_username = self.configuration.san_login
        self.storage_password = self.configuration.san_password
        self.client = client.KSClient(self.active_storage_ip,
                                      self.storage_username,
                                      self.storage_password,
                                      self.configuration)
        self.stats = {}
        # conf_pools = self.configuration.safe_get("storage_pool_name")

    def create_consistencygroup(self, group):
        """Creates a consistency group."""
        pass

    def delete_consistencygroup(self, group, volumes):
        """Deletes a consistency group."""
        pass

    def update_consistencygroup(self, group, add_volumes, remove_volumes):
        """Adds or removes LUN(s) to/from an existing consistency group"""
        pass

    def create_cgsnapshot(self, cgsnapshot, snapshots):
        """Creates a cgsnapshot (snap group)."""
        pass

    def delete_cgsnapshot(self, cgsnapshot, snapshots):
        """Deletes a cgsnapshot (snap group)."""
        pass

    def create_volume(self, volume, lun):
        pool = self.configuration.safe_get("storage_pool_name")
        try:
            lun = self.client.create_volume(volume, pool, lun)
        except exception as exc:
            LOG.error(_LE("create volume: %(volume)s, reason is %(err)s"),
                      {'volume': volume.name, 'err': exc.message})
            raise exc

        vol_path = "type:lun-%s" % str(lun['id'])
        model_update = {'provider_location': vol_path}
        return model_update

    def create_volume_from_snapshot(self, volume, snapshot, lun):
        # Creates a volume from a Unity snapshot.
        pool = self.configuration.safe_get("storage_pool_name")
        try:
            flatten = self.configuration.\
                safe_get("flatten_volume_from_snapshot")
            lun = self.client.create_volume_from_snapshot(volume, snapshot,
                                                          lun, pool, flatten)
        except Exception as exc:
            LOG.error(_LE("create volume: %(volume)s from snap: "
                          "%(snap)s failed, reason is %(err)s"),
                      {'volume': volume.name, 'snap': snapshot.name,
                       'err': exc.message})
            raise exc

        vol_path = "type:lun-%s" % str(lun['id'])
        model_update = {'provider_location': vol_path}
        return model_update

    def create_cloned_volume(self, volume, src_vref, lun):
        """Creates cloned volume.

        1. Take an internal snapshot of source volume, and attach it.
        2. Create a new volume, and attach it.
        3. Copy from attached snapshot of step 1 to the volume of step 2.
        4. Delete the internal snapshot created in step 1.
        """
        pool = self.configuration.safe_get("storage_pool_name")
        snapshot_name = src_vref['name'] + '-temp-snapshot'
        snapshot = {
            'name': snapshot_name,
            'volume_name': src_vref['name'],
            'volume_size': src_vref['size'],
        }

        self.client.create_snapshot(snapshot, pool, is_temp=True)
        try:
            # Create volume
            flatten = self.configuration.\
                safe_get("flatten_volume_from_snapshot")
            lun = self.client.create_volume_from_snapshot(volume, snapshot,
                                                          lun, pool, flatten,
                                                          is_temp=True)
            vol_path = "type:lun-%s" % str(lun['id'])
            model_update = {'provider_location': vol_path}
            return model_update
        except Exception as exc:
            LOG.error(_LE("create clone volume: %(volume)s from snap: "
                          "%(snap)s failed, reason is %(err)s"),
                      {'volume': volume.name, 'snap': snapshot["name"],
                       'err': exc.message})
            raise exc
        finally:
            # Delete temp Snapshot
            self.client.delete_snapshot(snapshot, pool, is_temp=True)

    def delete_volume(self, volume):
        pool = self.configuration.safe_get("storage_pool_name")
        try:
            self.client.delete_volume(volume, pool)
        except Exception as exc:
            LOG.error(_LE("delete volume: %(volume)s failed, "
                          "reason is %(err)s"),
                      {'volume': volume.name, 'err': exc.message})
            raise exc

    def create_snapshot(self, snapshot):
        """This function will create a snapshot of the given volume."""
        pool = self.configuration.safe_get("storage_pool_name")
        try:
            self.client.create_snapshot(snapshot, pool)
        except Exception as exc:
            LOG.error(_LE('Create snapshot failed, '
                          '%(snapshot)s: volume: %(volume)s'),
                      {'snapshot': snapshot['name'],
                       'volume': snapshot['volume_name']})
            raise exc
        snap_path = "type:%s" % snapshot['name']
        model_update = {'provider_location': snap_path}
        return model_update

    def delete_snapshot(self, snapshot):
        """Gets the snap id by the snap name and delete the snapshot."""
        pool = self.configuration.safe_get("storage_pool_name")
        try:
            self.client.delete_snapshot(snapshot, pool)
        except Exception as exc:
            LOG.error(_LE('Delete snapshot failed, '
                          '%(snapshot)s: volume: %(volume)s'),
                      {'snapshot': snapshot['name'],
                       'volume': snapshot['volume_name']})
            raise exc

    def rollback_snapshot(self, volume, snapshot):
        pool = self.configuration.safe_get("storage_pool_name")
        try:
            self.client.rollback_snapshot(volume, snapshot, pool)
        except Exception as exc:
            LOG.error(_LE('rollbak snapshot failed, '
                          '%(snapshot)s: volume: %(volume)s'),
                      {'snapshot': snapshot['name'],
                       'volume': snapshot['volume_name']})
            raise exc

    def extend_volume(self, volume, new_size):
        pool = self.configuration.safe_get("storage_pool_name")
        try:
            self.client.extend_volume(volume, new_size, pool)
        except Exception as exc:
            LOG.error(_LE('extend volume: %(volume)s, reason is %(err)s'),
                      {'volume': volume.name, 'err': exc.message})
            raise exc

    def expose_lun(self, volume, lun_data, host_id, is_snap=False):
        pass

    def initialize_connection(self, volume, connector):
        try:
            connection = self.client.initialize_connection(volume)
        except Exception as exc:
            LOG.error(_LE('initialize volume: %(volume)s, reason is %(err)s'),
                      {'volume': volume.name, 'err': exc.message})
            raise exc
        return connection

    def terminate_connection(self, volume, connector, **kwargs):
        self.client.terminate_connection(volume, connector)

    def get_volume_stats(self, refresh=False):
        if refresh:
            self.update_volume_stats()
        return self.stats

    def update_volume_stats(self):
        LOG.debug("Updating volume stats")
        # Check if thin provisioning license is installed
        stats = {'driver_version': VERSION,
                 'free_capacity_gb': 'unknown',
                 'reserved_percentage': 0,
                 'total_capacity_gb': 'unknown',
                 'vendor_name': 'Open Source',
                 'volume_backend_name': None}
        backend_name = self.configuration.safe_get('volume_backend_name')
        stats['volume_backend_name'] = backend_name or 'RBDISCSIDriver'
        stats['storage_protocol'] = self.storage_protocol
        pools = self.client.get_volume_backend_info()
        conf_pool = self.configuration.safe_get("storage_pool_name")
        pool_stats = [pool for pool in pools if
                      pool['name'] == conf_pool][0]
        stats['free_capacity_gb'] = pool_stats['sizeFree']
        stats['total_capacity_gb'] = pool_stats['sizeTotal']
        self.stats = stats
        LOG.debug('Volume Stats: %s', stats)
        return self.stats

    def manage_existing_get_size(self, volume, ref):
        """Return size of volume to be managed by manage_existing."""
        pass

    def manage_existing(self, volume, ref):
        """Manage an existing lun in the array."""
        pass

    def get_volume_backend_info(self):

        LOG.info(_("Get volume backend info start"))
        backend_info = {}
        pools = self.client.get_volume_backend_info()
        conf_pool = self.configuration.safe_get("storage_pool_name")
        pool_stats = [pool for pool in pools if
                      pool['name'] == conf_pool][0]
        backend_info['Avail'] = str(pool_stats['sizeFree']) + "GB"
        backend_info['Used'] = str(pool_stats['sizeSubscribed']) + "GB"
        backend_info['Total'] = str(pool_stats['sizeTotal']) + "GB"
        backend_info['Usage'] = \
            str(round(float(pool_stats['sizeSubscribed']) /
                      pool_stats['sizeTotal'] * 100, 2))
        return backend_info


class RBDISCSIDriver(san.SanDriver):
    """RBD iSCSI Driver."""

    def __init__(self, *args, **kwargs):
        super(RBDISCSIDriver, self).__init__(*args, **kwargs)
        self.helper = RBDISCSIHelper(self.configuration, self)

    def check_for_setup_error(self):
        pass

    def create_consistencygroup(self, context, group):
        return self.helper.create_consistencygroup(group)

    def delete_consistencygroup(self, context, group, volumes):
        return self.helper.delete_consistencygroup(group, volumes)

    def update_consistencygroup(self, context, group,
                                add_volumes=None, remove_volumes=None):
        return self.helper.update_consistencygroup(group,
                                                   add_volumes, remove_volumes)

    def create_cgsnapshot(self, context, cgsnapshot, snapshots):
        return self.helper.create_cgsnapshot(cgsnapshot, snapshots)

    def delete_cgsnapshot(self, context, cgsnapshot, snapshots):
        return self.helper.delete_cgsnapshot(cgsnapshot, snapshots)

    def create_volume(self, volume, lun, **kwargs):
        """Creates a volume."""
        return self.helper.create_volume(volume, lun)

    def create_volume_from_snapshot(self, volume, snapshot, lun):
        """Creates a volume from a snapshot."""
        return self.helper.create_volume_from_snapshot(volume, snapshot, lun)

    def create_cloned_volume(self, volume, src_vref, lun):
        """Creates a cloned volume."""
        return self.helper.create_cloned_volume(volume, src_vref, lun)

    def delete_volume(self, volume):
        """Deletes a volume."""
        return self.helper.delete_volume(volume)

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        LOG.info(_LI('Create snapshot: %(snapshot)s: volume: %(volume)s'),
                 {'snapshot': snapshot['name'],
                  'volume': snapshot['volume_name']})

        return self.helper.create_snapshot(snapshot)

    def delete_snapshot(self, snapshot):
        """Delete a snapshot."""
        LOG.info(_LI('Delete snapshot: %s'), snapshot['name'])
        return self.helper.delete_snapshot(snapshot)

    def rollback_snapshot(self, volume, snapshot):
        """Revert a snapshot."""
        LOG.info(_LI('Rollback snapshot: %s'), snapshot['name'])
        return self.helper.rollback_snapshot(volume, snapshot)

    def extend_volume(self, volume, new_size):
        """Extend a volume."""
        return self.helper.extend_volume(volume, new_size)

    def initialize_connection(self, volume, connector):
        return self.helper.initialize_connection(volume, connector)

    def terminate_connection(self, volume, connector, **kwargs):
        return self.helper.terminate_connection(volume, connector)

    def get_volume_stats(self, refresh=False):
        return self.helper.get_volume_stats(refresh)

    def update_volume_stats(self):
        return self.helper.update_volume_stats()

    def manage_existing_get_size(self, volume, existing_ref):
        """Return size of volume to be managed by manage_existing."""
        return self.helper.manage_existing_get_size(
            volume, existing_ref)

    def manage_existing(self, volume, existing_ref):
        return self.helper.manage_existing(
            volume, existing_ref)

    def unmanage(self, volume):
        pass

    def get_volume_backend_info(self):
        return self.helper.get_volume_backend_info()
