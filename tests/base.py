# coding: utf-8

import os
import time
import uuid
import json
import shutil
import unittest

from tests.dbclient import MCStore
from tests.utils import (
    start_cmd, stop_cmd, get_server_addr, load_yaml, gethttp
)
from tests import gen_config


GOBEANSDB_CMD = "gobeansdb"
GOBEANSPROXY_CMD = f"{os.environ['GOPATH']}/bin/gobeansproxy"
GOBEANSDB_READ_ENABLE = os.environ.get("GOBEANSPROXY_TEST_BR") == "1"
GOBEANSDB_WRITE_ENABLE = os.environ.get("GOBEANSPROXY_TEST_BW") == "1"
CSTAR_READ_ENABLE = os.environ.get("GOBEANSPROXY_TEST_CR") == "1"
CSTAR_WRITE_ENABLE = os.environ.get("GOBEANSPROXY_TEST_CW") == "1"


class BaseTest(unittest.TestCase):
    def setUp(self):
        root_dir = '/tmp/gobeansproxy_%s/' % uuid.uuid4()
        self.bdb_read_enable = GOBEANSDB_READ_ENABLE
        self.bdb_write_enable = GOBEANSDB_WRITE_ENABLE
        self.cstar_read_enable = CSTAR_READ_ENABLE
        self.cstar_write_enable = CSTAR_WRITE_ENABLE
        gen_config.gen_conf(
            root_dir,
            bdb_read_enable=self.bdb_read_enable,
            bdb_write_enable=self.bdb_write_enable,
            cstar_read_enable=self.cstar_read_enable,
            cstar_write_enable=self.cstar_write_enable
        )

        self.dbs = [GobeansdbInstance(os.path.join(root_dir, str(port), 'conf'))
                    for (port, _) in gen_config.MAIN_PORT_PAIRS]
        for db in self.dbs:
            db.start(self.bdb_read_enable)

        self.backup_dbs = [GobeansdbInstance(os.path.join(root_dir, str(port), 'conf'))
                           for (port, _) in gen_config.BACKUP_PORT_PAIRS]
        for db in self.backup_dbs:
            db.start(self.bdb_read_enable)

        self.proxy = GobeansproxyInstance(
            os.path.join(root_dir, 'proxy', 'conf'))
        self.proxy.start(self.bdb_read_enable)

    def tearDown(self):
        # time.sleep(1000)
        self.proxy.clean()
        for db in self.dbs:
            db.clean()
        for db in self.backup_dbs:
            db.clean()

    def checkCounterZero(self):
        if not (self.bdb_read_enable or self.bdb_write_enable):
            return
        time.sleep(0.5)
        content = gethttp(self.proxy.webaddr, 'buffer')
        buffers = json.loads(content)
        self.assertEqual(len(buffers), 4)
        for _, v in list(buffers.items()):
            self.assertEqual(v['Count'], 0, content)
            self.assertEqual(v['Size'], 0, content)

    @classmethod
    def require_rw_enable(func, br, bw, cr, cw):
        def wrap(func):
            def check_rw_func(*args, **kwargs):
                if not (GOBEANSDB_READ_ENABLE in br and \
                        GOBEANSDB_WRITE_ENABLE in bw and \
                        CSTAR_READ_ENABLE in cr and \
                        CSTAR_WRITE_ENABLE in cw):
                    return
                return func(*args, **kwargs)
            return check_rw_func

        return wrap
            


class BaseServerInstance(object):
    def __init__(self, conf_dir, bin, server_name):
        self.popen = None
        self.cmd = '%s -confdir %s' % (bin, conf_dir)
        self.addr, self.webaddr = get_server_addr(conf_dir, server_name)

    def start(self, bdb_read_enable=True):
        assert self.popen is None
        self.popen = start_cmd(self.cmd)
        try_times = 0
        while True:
            try:
                store = MCStore(self.addr)
                if bdb_read_enable:
                    store.get("@")
                else:
                    store.set("test", "test")
                return
            except IOError:
                try_times += 1
                if try_times > 20:
                    raise Exception("connect error for addr: %s", self.addr)
                time.sleep(0.5)

    def stop(self):
        print('stop', self.cmd)
        if self.popen:
            stop_cmd(self.popen)
            self.popen = None

    def clean(self):
        if self.popen:
            self.stop()
        self.clean_data()

    def clean_data(self):
        pass


class GobeansproxyInstance(BaseServerInstance):
    def __init__(self, conf_dir):
        super(GobeansproxyInstance, self).__init__(
            conf_dir, GOBEANSPROXY_CMD, 'gobeansproxy')


class GobeansdbInstance(BaseServerInstance):
    def __init__(self, conf_dir):
        super(GobeansdbInstance, self).__init__(
            conf_dir, GOBEANSDB_CMD, 'gobeansdb')
        conf = load_yaml(os.path.join(conf_dir, 'global.yaml'))
        self.db_home = conf['hstore']['local']['home']

    def clean_data(self):
        if os.path.exists(self.db_home):
            shutil.rmtree(self.db_home)
