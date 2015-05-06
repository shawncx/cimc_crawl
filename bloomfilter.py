import threading
from pybloom import ScalableBloomFilter

__author__ = 'chen_xi'

class SyncScalableBloomFilter(ScalableBloomFilter):

    _lock = threading.RLock()

    def put_not_existed_keys(self, keys, queue):
        with SyncScalableBloomFilter._lock:
            [queue.put(key) for key in keys if not self.add(key)]



