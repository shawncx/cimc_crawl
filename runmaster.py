from configparser import ConfigParser, NoSectionError

import const
from crawler import CrawlerMaster


__author__ = 'chen_xi'

handler_delegates = {}

def handle_grab(db_conn, ret_args):
    pass


handler_delegates[const.CMD_GRAB] = handle_grab

if __name__ == '__main__':
    cfg = ConfigParser()
    cfg.read('master_config.ini')

    master_id = cfg.get('master', 'id')
    master_ip = cfg.get('master', 'ip')
    master_port = cfg.getint('master', 'port')

    router = {}
    router.update({master_id: (master_ip, master_port)})

    for i in range(10):
        try:
            slave_id = cfg.get('slave_' + str(i), 'id')
            slave_ip = cfg.get('slave_' + str(i), 'ip')
            slave_port = cfg.getint('slave_' + str(i), 'port')
            router.update({slave_id: (slave_ip, slave_port)})
        except NoSectionError:
            break

    master = CrawlerMaster(master_id, router)
    master.launch(handler_delegates)
    master.start_grab('http://www.worksap.com/', 'utf-8')
    # master.start_grab('http://bbs.hupu.com/12628258.html', 'gbk')
