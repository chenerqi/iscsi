iSCSI Client
====================
This is a Client library that can talk to the TGT iSCSI Storage Cluster. 
The library access iSCSI server via SSH and iSCSI server provides 
block device servie with Ceph rbd backingstore.

The python paramiko library is used to communicate with 
the iSCSI server over an SSH connection.

Requirements
============

Capabilities
============
* Create Volume
* Delete Volume
* Extend Volume
* Clone a Volume
* Create a Volume Snapshot
* Delete a Volume Snapshot
* Rollback a Volume Snapshot
* Get Volume Info
* Get IP Portals
* Get Storage pool Info

Installation
============

No need installation

Folders
=======

* iscsiclient -- the actual client.py library
* samples -- some sample uses