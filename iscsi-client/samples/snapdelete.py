import os, sys
# this is a hack to get the ks driver module
# and it's utils module on the search path.
client_folder = os.path.realpath(os.path.abspath("..") )
if client_folder not in sys.path:
     sys.path.insert(0, client_folder)

from iscsiclient import client
import vs

username = 'root'
password = 'KingSoft123!'
ip = '10.210.0.66'

if __name__ == "__main__":
    cl = client.KSClient(ip, username, password)
    try:
        import pdb;pdb.set_trace()
        snapshot = vs.Snapshot(volume_name='image01', snap_name='snap1')
        cl.delete_snapshot(snapshot, 'rbd', False) 
    except Exception as e:
         print e
