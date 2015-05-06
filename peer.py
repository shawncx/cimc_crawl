import json
from socket import AF_INET, socket, SOCK_STREAM
from socketserver import BaseRequestHandler, ThreadingTCPServer
import threading
import const

DEBUG = False

class Peer:
    def __init__(self, id, router, RequestHandlerClass, instance ,buffer_size=8192000):
        '''
        :param id: Peer id
        :param router: Dict for peer router. {PeerId: ('ip', port)}
        :param handler: Request handler
        :param instance: Instance
        :param processor_delegates: Open delegates for customization
        :param buffer_size: Message buffer
        :return:
        '''
        self.id = id
        self.ip, self.port = router[id]
        self.buffer_size = buffer_size
        self.RequestHandlerClass = RequestHandlerClass
        self.instance = instance
        self.router = router

        self.connected_peers = {}
        self.launched = False

    def launch(self, is_target_url=None, handler_delegates={}):
        self.serv = PeerServer(('', self.port), self.RequestHandlerClass, self.instance, is_target_url, handler_delegates)
        self.launched = True
        self.t_server = threading.Thread(target=self.__launch)
        self.t_server.start()

    def __launch(self):
        self.serv.serve_forever()

    def conn_peer(self, target_id):
        if not self.is_launched() or not self.is_in_router(target_id):
            raise Exception('Server not been launched or Target Server not in router')
        conn = socket(AF_INET, SOCK_STREAM)
        conn.connect((self.router[target_id]))
        self.connected_peers[target_id] = conn

    def send_msg(self, target_id, msg):
        if not self.is_launched() or not self.is_in_router(target_id) or not self.is_connected(target_id):
            return
        conn = self.connected_peers[target_id]
        conn.send(msg)
        return conn.recv(self.buffer_size)

    def disconnect(self, target_id):
        if not self.is_launched() or not self.is_in_router(target_id) or not self.is_connected(target_id):
            return
        conn = self.connected_peers[target_id]
        conn.close()
        self.connected_peers.pop(target_id)

    def shutdown(self):
        if not self.is_launched():
            return
        for id in list(self.connected_peers.keys()):
            self.disconnect(id)
        self.serv.shutdown()
        self.launched = False

    def is_launched(self):
        if not self.launched:
            msg = 'Source peer has not launched! Please call launch().'
        else:
            msg = 'Source peer has launched.'
        if DEBUG:
            print(msg)
        return self.launched

    def is_in_router(self, id):
        if id not in self.router.keys():
            msg  = 'Peer %s is not in the router'% (id, )
        else:
            msg  = 'Peer %s is in the router' % (id, )
        if DEBUG:
            print(msg)
        return id in self.router.keys()

    def is_connected(self, id):
        if id not in self.connected_peers.keys():
            msg = 'Peer %s has not connected!' % (id, )
        else:
            msg = 'Peer %s has connected!' % (id, )
        if DEBUG:
            print(msg)
        return id in self.connected_peers.keys()


class PeerServer(ThreadingTCPServer):

    def __init__(self, server_address, RequestHandlerClass, instance, is_target_url, handler_delegates):
        super().__init__(server_address,RequestHandlerClass)
        self.instance = instance
        self.is_target_url = is_target_url
        self.handler_delegates = handler_delegates
