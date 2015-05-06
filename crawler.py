from datetime import datetime
import json
from queue import Queue
from socketserver import BaseRequestHandler
import threading
import time
import urllib.request
from wsgiref import headers
import sqlite3
import bs4
from bloomfilter import SyncScalableBloomFilter
import const
from peer import Peer

__author__ = 'chen_xi'

MAX_PAGE = None

class Crawler:
    def __init__(self, id, router):
        self.id = id
        self.ip, self.port = router[id]
        self.server = Peer(id, router, CommandHandler, self)
        self.status = const.CRAWLER_STATUS_OFFLINE

    def launch(self, is_target_url=None, handler_delegates={}):
        self.server.launch(is_target_url, handler_delegates)

    def exe_cmd(self, target_id, cmd):
        print('Send command:', cmd.name, ' with args', cmd.args, ' to Crawler: ', target_id)
        j_cmd = Command.to_json(cmd)
        try:
            ret = self.server.send_msg(target_id, str.encode(j_cmd))
            if not ret:
                return CommandResult(const.CMD_RET_UNKNOW)
            else:
                j_result = ret.decode()
                return CommandResult.to_object(j_result)
        except Exception as e:
            return CommandResult(const.CMD_RET_EXCEPTION, [str(e), ])


class CrawlerMaster(Crawler):
    def __init__(self, id, router):
        super().__init__(id, router)

        # Init slaves
        self.slaves = {}
        for slave_id in [s_id for s_id in router.keys() if s_id != self.id]:
                self.slaves[slave_id] = CrawlerSlaveClient(slave_id, router, id)

    def launch(self, handler_delegates={}):
        super().launch()
        self.status = const.CRAWLER_STATUS_RUNNING
        self.sync_slaves()
        self.handler_delegates = handler_delegates

    def sync_slaves(self):
        unsync_slaves = list([value for value in self.slaves.values() if value.status != const.CRAWLER_STATUS_RUNNING])
        while len(unsync_slaves) > 0:
            # Always get first slave
            id, slave = unsync_slaves[0].id, unsync_slaves[0]
            slave.reconn_time += 1

            if self.exe_conn(id).status == const.CMD_RET_SUCCESS and self.exe_ping(id).status == const.CMD_RET_SUCCESS:
                # Has exceed reconnected time threshold, give up this slave
                # Establish connection successfully
                slave.status = const.CRAWLER_STATUS_RUNNING

                # Reset reconn_time
                slave.reconn_time = 0

                print('Sync Slave: ', id, 'success!')
                del unsync_slaves[0]

            else:
                print('Sync Slave: ', id, 'fail! Has tried ', str(slave.reconn_time), ' time!')
                if slave.reconn_time > const.CRAWLER_RECONN_TIME:
                    # Has exceed reconnected time threshold, give up this slave
                    print('Sync Slave: ', id, 'has exceed threshold time! Give up this slave!')
                    slave.status = const.CRAWLER_STATUS_UNKNOW
                    del unsync_slaves[0]

                else:
                    # Wait some time to re connect
                    time.sleep(const.CRAWLER_RECONN_WAIT)

        available_slaves = [value for value in self.slaves.values() if value.status == const.CRAWLER_STATUS_RUNNING]
        print('Available Slaves Number is: %d' % len(available_slaves))
        if len(available_slaves) == 0:
            print('No Available Slave! Shutdown Server!')
            self.shutdown()

    def shutdown(self):
        self.server.shutdown()
        self.status = const.CRAWLER_STATUS_OFFLINE

    def start_grab(self, url, coding):
        self.init_db()

        # url queue. Waiting for grabbing
        url_q = Queue()
        url_q.put(url)

         # Init pool
        standby_slaves = []
        [standby_slaves.append(slave) for slave in self.slaves.values() if slave.status == const.CRAWLER_STATUS_RUNNING]
        slave_pool = WorkerManager(standby_slaves)

        # urls have been grabbed.
        visited_urls = SyncScalableBloomFilter()

        if MAX_PAGE:
            for count in range(MAX_PAGE):
                cur_url = url_q.get()
                visited_urls.add(cur_url)
                cmd = Command(const.CMD_GRAB, [cur_url, coding])
                print('Begin to process %s' % cur_url)
                slave_pool.submit_task(self.exe_grap, cmd, url_q, visited_urls)
        else:
            while True:
                cur_url = url_q.get()
                print('%d url in Queue' % url_q.qsize())
                visited_urls.add(cur_url)
                cmd = Command(const.CMD_GRAB, [cur_url, coding])
                print('Begin to process %s' % cur_url)
                slave_pool.submit_task(self.exe_grap, cmd, url_q, visited_urls)

    def exe_conn(self, slave_id):
        try:
            self.server.conn_peer(slave_id)
            return CommandResult(const.CMD_RET_SUCCESS, )
        except Exception as e:
            return CommandResult(const.CMD_RET_EXCEPTION, [str(e), ])

    def exe_ping(self, slave_id):
        return self.exe_cmd(slave_id, Command(const.CMD_PING))

    def exe_grap(self, slave, db_conn, cmd, url_q, visited_urls):
        ret = self.exe_cmd(slave.id, cmd)
        if ret.status == const.CMD_RET_SUCCESS:
            print('Got grab result from ', slave.id)
            # print(ret.args[1])

            self.save_grab(db_conn, slave.id, cmd.args[0], ret.args[1])

            # If find new urls, add them into waiting queue
            if ret.args and ret.args[0]:
                visited_urls.put_not_existed_keys(ret.args[0], url_q)

            # Reset reconnect time
            slave.reconn_time = 0

            if self.handler_delegates.get(const.CMD_GRAB, None):
                self.handler_delegates.get(const.CMD_GRAB)(db_conn, ret.args)

        elif ret.status == const.CMD_RET_EXCEPTION:
            print('Slave ', slave.id, ' got exception, ', ret.args[0])

            # Grap fail, return the url to available queue
            url_q.put(cmd.args[0])

            slave.reconn_time += 1
            if slave.reconn_time > const.CRAWLER_RECONN_TIME:
                # If exceed threshold, not put it into available queue again
                print('Conn Slave: ', id, 'has exceed threshold time! Give up this slave!')
                slave.status = const.CRAWLER_STATUS_UNKNOW
                raise Exception('Error', 'Conn Slave: ', id, 'has exceed threshold time! Give up this slave!')

    def init_db(self):
        conn = sqlite3.connect(const.DB)
        drop_tb = 'DROP TABLE page'
        create_tb = 'CREATE TABLE page (id integer PRIMARY KEY, slave_id text, url text, content text, prc_date timestamp)'
        try:
            self.exe_sql(conn, drop_tb)
        except Exception as e:
            print(str(e))
        self.exe_sql(conn, create_tb)


    def save_grab(self, conn, slave_id, url, content):
        insert = 'INSERT INTO page VALUES (NULL, ?, ?, ?, ?)'
        self.exe_sql(conn, insert, (slave_id, url, content, datetime.now()))

    def exe_sql(self, conn, sql, args=None):
        cur = conn.cursor()
        if args:
            res = cur.execute(sql, args)
        else:
            res = cur.execute(sql)
        conn.commit()
        cur.close()
        return res



class CrawlerSlave(Crawler):
    def __init__(self, id, router, master_id):
        super().__init__(id, router)
        self.master_id = master_id

    def launch(self, is_target_url, handler_delegates={}):
        super().launch(is_target_url, handler_delegates)
        self.status = const.CRAWLER_STATUS_RUNNING
        print('Slave: ', self.id, 'Listening port: ', self.port)

class CrawlerSlaveClient(CrawlerSlave):
    '''Slave client at Master side. With some additional information which is used to tag slave
    '''
    def __init__(self, id, router, master_id):
        super().__init__(id, router, master_id)
        self.reconn_time = 0


class WorkerManager:
    def __init__(self, slaves):
        self.task_q = Queue()
        self.threads = []
        self.init_thread_pool(slaves)

    def init_thread_pool(self, slaves):
        [self.threads.append(Worker(slave, self.task_q)) for slave in slaves]

    def submit_task(self, func, *args):
        self.task_q.put((func, list(args)))


class Worker(threading.Thread):
    def __init__(self, slave, task_q):
        threading.Thread.__init__(self)
        self.slave = slave
        self.task_q = task_q
        self.start()

    def run(self):
        self.db_conn = sqlite3.connect(const.DB)
        while True:
            try:
                do, args = self.task_q.get()
                do(self.slave, self.db_conn, *args)
                self.task_q.task_done()
            except Exception as e:
                print('End thread: ', str(e))
                self.db_conn.close()
                break


class Command:
    def __init__(self, name, args=[]):
        self.name = name
        self.args = args

    @staticmethod
    def to_json(cmd):
        d = {'name': cmd.name, 'args': cmd.args}
        return json.dumps(d)

    @staticmethod
    def to_object(json_str):
        d = json.loads(json_str)
        return Command(d['name'], d['args'])

class CommandResult:
    def __init__(self, status, args=[]):
        self.status = status
        self.args = args

    @staticmethod
    def to_json(cmd_result):
        d = {'status': cmd_result.status, 'args': cmd_result.args}
        return json.dumps(d)

    @staticmethod
    def to_object(json_str):
        d = json.loads(json_str)
        return CommandResult(d['status'], d['args'])


class CommandHandler(BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.handlers = {}
        self.handlers[const.CMD_PING] = self.handle_ping
        self.handlers[const.CMD_GRAB] = self.handle_grab
        super().__init__(request, client_address, server)

    def handle(self):
        while True:
            b_cmd = self.request.recv(8192)
            cmd = Command.to_object(b_cmd.decode())
            # Special process
            handler = self.handlers.get(cmd.name)
            if handler:
                ret = handler(cmd)
                j_ret = CommandResult.to_json(ret)
                self.request.send(str.encode(j_ret))

    def handle_ping(self, cmd):
        print('Here is Crawler ', self.server.instance.id)
        return CommandResult(const.CMD_RET_SUCCESS)

    def handle_grab(self, cmd):
        url = cmd.args[0]
        coding = cmd.args[1]
        print('Here is Crawler ', self.server.instance.id, 'I will grab ', url)
        hrefs = []
        try:
            opener = urllib.request.build_opener()
            opener.add_handler = headers
            content = opener.open(url).read()
            s_content = content.decode(encoding=coding)
            hrefs = self.find_href(content, coding)
            if self.server.handler_delegates.get(const.CMD_GRAB, None):
                self.server.handler_delegates.get(const.CMD_GRAB)(self.server.instance, url, s_content)
        except Exception as e:
            s_content = str(e)
            print(s_content)
        finally:
            return CommandResult(const.CMD_RET_SUCCESS, [hrefs, s_content])

    def find_href(self, content, coding):
        hrefs = set()
        try:
            soup = bs4.BeautifulSoup(content, from_encoding=coding)
            t_as = soup.find_all('a')
            for t_a in t_as:
                h = t_a.get('href', None)
                if h:
                    url = self.server.is_target_url(h)
                    if url:
                        hrefs.add(url)
        except Exception as e:
            pass
        finally:
            return list(hrefs)