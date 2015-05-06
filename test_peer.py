import threading
import time
import const
from peer import Peer

__author__ = 'chen_xi'

router = {'p1': ('localhost', 9000),
          'p2': ('localhost', 9001)}

p1 = Peer('p1', router)
p2 = Peer('p2', router)

t1 = threading.Thread(target=p1.launch)
t2 = threading.Thread(target=p2.launch)

t1.start()
t2.start()

time.sleep(4)

p2.conn_peer('p1')
p1.conn_peer('p2')

recv_msg1 = p2.send_msg('p1', const.MSG_PING)
recv_msg2 = p1.send_msg('p2',const.MSG_PING)
print(recv_msg1)
print(recv_msg2)

p1.shutdown()
p2.shutdown()
