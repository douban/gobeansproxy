# coding: utf-8

from tests.base import BaseTest
from tests.dbclient import MCStore


class KeyVersionTest(BaseTest):
    def setUp(self):
        BaseTest.setUp(self)

    # def tearDown(self):
    #     import time
    #     time.sleep(600)
    #     super(self).tearDown()

    def _assert_val(self, addr, key, val, msg=None):
        store = MCStore(addr)
        self.assertEqual(store.get(key), val, msg)

    @BaseTest.require_rw_enable(
        br=(True, False), bw=(True,), cr=(False,), cw=(False, True)
    )
    def test_normal(self):
        proxy = MCStore(self.proxy.addr)
        key = '/test/normal/key'
        val = 'val1'

        self.assertTrue(proxy.set(key, val))
        self.assertEqual(proxy.get(key), val)
        for db in self.dbs:
            self._assert_val(db.addr, key, val)

    @BaseTest.require_rw_enable(
        br=(True, False), bw=(True,), cr=(False,), cw=(False, True)
    )
    def test_one_server_down(self):
        proxy = MCStore(self.proxy.addr)
        key = '/test/one/server/down'
        val = 'val'
        bad_server_idx = 0

        self.dbs[bad_server_idx].stop()
        self.assertTrue(proxy.set(key, val))
        self.assertEqual(proxy.get(key), val)
        for i, db in enumerate(self.dbs):
            if i != bad_server_idx:
                self._assert_val(db.addr, key, val)

        # 没有写备节点
        for db in self.backup_dbs:
            self._assert_val(db.addr, key, None)

    @BaseTest.require_rw_enable(
        br=(True, False), bw=(True,), cr=(False,), cw=(False, True)
    )
    def test_two_server_down(self):
        proxy = MCStore(self.proxy.addr)
        key = '/test/two/server/down'
        val = 'val'
        bad_server_idxs = [0, 1]

        for i in bad_server_idxs:
            self.dbs[i].stop()
        self.assertTrue(proxy.set(key, val))
        self.assertEqual(proxy.get(key), val)
        for i, db in enumerate(self.dbs):
            if i not in bad_server_idxs:
                self._assert_val(db.addr, key, val)

        # 写备节点了. 这里假设测试配置中只有一个备节点
        for db in self.backup_dbs:
            self._assert_val(db.addr, key, val)

    @BaseTest.require_rw_enable(
        br=(True, False), bw=(True,), cr=(False,), cw=(False, True)
    )
    def test_three_server_down(self):
        proxy = MCStore(self.proxy.addr)
        key = '/test/three/server/down'
        val = 'val'
        bad_server_idxs = [0, 1, 2]

        for i in bad_server_idxs:
            self.dbs[i].stop()
        self.assertFalse(proxy.set(key, val))
        with self.assertRaises(IOError):
            proxy.get(key)

        for i, db in enumerate(self.dbs):
            if i not in bad_server_idxs:
                self._assert_val(db.addr, key, val)

        # 写备节点了
        for db in self.backup_dbs:
            self._assert_val(db.addr, key, val)
