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
ip = '10.210.3.20'

if __name__ == "__main__":
    cl = client.KSClient(ip, username, password)
    volume = vs.Volume(name='image01')
    try:
        import pdb;pdb.set_trace()
        out = cl.create_volume(volume, 'rbd', 100) 
        print out

        new_size = 12
        #volume.set_id(out['id']) 
        cl.extend_volume(volume, new_size, 'rbd')
    except Exception as e:
         print e