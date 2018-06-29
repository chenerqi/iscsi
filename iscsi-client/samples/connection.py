import os, sys
# this is a hack to get the ks driver module
# and it's utils module on the search path.
client_folder = os.path.realpath(os.path.abspath("..") )
if client_folder not in sys.path:
     sys.path.insert(0, client_folder)

from iscsiclient import client
import vs

username = 'root'
password = '123456'
#ip = '10.210.0.67,10.210.0.68,10.210.0.69'
ip = '192.168.47.128'

if __name__ == "__main__":
    cl = client.KSClient(ip, username, password)
    try:
        import pdb;pdb.set_trace()
        volume = vs.Volume()

        out = cl.initialize_connection(volume)
        print out

        cl.terminate_connection(volume)

        out = cl.get_volume_backend_info()
        print out
    except Exception as e:
         print e
