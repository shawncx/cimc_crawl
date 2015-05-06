from configparser import ConfigParser, NoSectionError
import re
import const
from crawler import CrawlerSlave

__author__ = 'chen_xi'

CURRENT_SLAVE_ID_0 = 'slave_0'

handler_delegates = {}

def handle_grab(slave, url, content):
    print('Here is Crawler ', slave.id, 'I have grabed ', url)
    # soup=bs4.BeautifulSoup(content)
    # t_as = soup.findAll('a')
    # for t_a in t_as:
    #     print(t_a['href'])


handler_delegates[const.CMD_GRAB] = handle_grab

p1 = re.compile(r'http://bbs.hupu.com/(bxj(\-\d+)?|\d+\.html)$')
p = re.compile(r'worksap')

def is_target_url(url):
    if url.startswith('http://') and p.search(url):
        if url.startswith('http://twitter') or url.startswith('http://www.facebook'):
            return None
        else:
            return url
    elif url.startswith('/'):
        return 'http://www.worksap.com' + url
    else:
        return None

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

    slave_0 = CrawlerSlave(CURRENT_SLAVE_ID_0, router, master_id)

    slave_0.launch(is_target_url, handler_delegates)
