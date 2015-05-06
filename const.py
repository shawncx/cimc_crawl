import sys

__author__ = 'chen_xi'

class _const:

    class ConstError(TypeError):
        pass

    def __setattr__(self, key, value):
        if self.__dict__.has_key(key):
            raise self.ConstError
        self.__dict__[key] = value

    DB = 'crawl_db.db3'

    CRAWLER_STATUS_UNKNOW = 'Unknow'
    CRAWLER_STATUS_OFFLINE = 'Offline'
    CRAWLER_STATUS_RUNNING = 'Running'

    # Slave Reconnect time threshold
    CRAWLER_RECONN_TIME = 3
    # After connect fail, waiting interval
    CRAWLER_RECONN_WAIT = 2

    # Command Message
    CMD_PING = 'Ping'
    CMD_GRAB = 'Grab'

    # Return
    CMD_RET_SUCCESS = 'Success'
    CMD_RET_FAIL = 'Fail'
    CMD_RET_UNKNOW = 'Unkown'
    CMD_RET_EXCEPTION = 'Exception'

sys.modules[__name__] = _const



