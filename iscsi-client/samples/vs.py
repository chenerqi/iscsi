class Volume(object):
    """ The volume for test. """
    def __init__(self, name='image01', id=1, pool='rbd'):
        self.name = name
        self.size = 10
        self.provider_location = 'id-100'
        self.host = 'controller1.ksc@ks_pool#%s' % (pool)
        print self.__dict__

class Snapshot(object):
    """ The snapshot for test. """
    def __init__(self, volume_name='image01', snap_name='snap1'):
        self.volume = Volume()
        self.volume_name = volume_name
        self.name = snap_name
        print self.__dict__
