# coding: utf-8

import os

from tests.base import BaseTest
from tests.dbclient import MCStore
from tests.utils import random_string


VERSION, HASH, FLAG, SIZE, TIMESTAMP, CHUNKID, OFFSET = list(range(7))

class KeyVersionTest(BaseTest):
    def setUp(self):
        BaseTest.setUp(self)

        self.last_pos = 0
        self.last_size = 0

    def update_pos(self, size):
        self.last_pos += self.last_size
        self.last_size = size

    def get_meta(self, store, key):
        meta = store.get("??" + key)
        if meta:
            meta = meta.split()
            assert(len(meta) == 7)
            return tuple([int(meta[i]) for i in [VERSION, CHUNKID, OFFSET]])

    @BaseTest.require_rw_enable(br=(True,), bw=(True,), cr=(False,), cw=(True, False))
    def test_incr(self):
        store = MCStore(self.proxy.addr)
        key = 'key1'
        store.incr(key, 10)
        self.assertEqual(store.get(key), 10)
        self.checkCounterZero()

    @BaseTest.require_rw_enable(br=(True,), bw=(True,), cr=(False,), cw=(False,))
    def test_set_version(self):
        store = MCStore(self.proxy.addr)
        key = 'key1'
        store.set(key, 'aaa')
        self.update_pos(256)

        self.assertEqual(store.get(key), 'aaa')
        self.assertEqual(self.get_meta(store, key), (1, 0, self.last_pos))

        store.set_raw(key, b'bbb', rev=3)
        self.update_pos(256)
        self.assertEqual(self.get_meta(store, key), (3, 0, self.last_pos))

        store.set_raw(key, b'bbb', rev=4)
        self.assertEqual(self.get_meta(store, key), (4, 0, self.last_pos))

        store.set_raw(key, b'ccc', rev=2)
        self.assertEqual(store.get(key), b'bbb')
        self.assertEqual(self.get_meta(store, key), (4, 0, self.last_pos))

        self.checkCounterZero()

    @BaseTest.require_rw_enable(br=(True,), bw=(True,), cr=(False,), cw=(False,))
    def test_delete_version(self):
        store = MCStore(self.proxy.addr)
        key = 'key1'

        store.set(key, 'aaa')
        self.update_pos(256)
        self.assertEqual(self.get_meta(store, key), (1, 0, self.last_pos))

        store.delete(key)
        self.update_pos(256)
        self.assertEqual(store.get(key), None)

        self.assertEqual(self.get_meta(store, key), (-2, 0, self.last_pos))
        self.checkCounterZero()

        store.set(key, 'bbb')
        self.update_pos(256)
        self.assertEqual(store.get(key), 'bbb')
        self.assertEqual(self.get_meta(store, key), (3, 0, self.last_pos))
        self.checkCounterZero()

    def test_special_key(self):
        store = MCStore(self.proxy.addr)
        kvs = [('a' * 200, 1), ('a', list(range(1000)))]
        for k, v in kvs:
            self.assertTrue(store.set(k, v))
            self.assertEqual(store.get(k), v)

        # restart
        self.proxy.stop()
        self.proxy.start()
        store = MCStore(self.proxy.addr)
        for (k, v) in kvs:
            v2 = store.get(k)
            self.assertEqual(v2, v, "key %s, value %s, not %s" % (k, v, v2))
        if not self.cstar_write_enable:
            self.checkCounterZero()

    def test_big_v(self):
        store = MCStore(self.proxy.addr)
        key = 'largekeykk'
        size = 10 * 1024 * 1024
        string_large = random_string(size // 10) * 10

        self.assertTrue(store.set(key, string_large))
        self.assertEqual(store.get(key), string_large)

    def test_env(self):
        self.assertEqual(
            os.environ.get("GOBEANSPROXY_TEST_BR") == "1",
            self.bdb_read_enable
        )
        self.assertEqual(
            os.environ.get("GOBEANSPROXY_TEST_BW") == "1",
            self.bdb_write_enable
        )
        self.assertEqual(
            os.environ.get("GOBEANSPROXY_TEST_CR") == "1",
            self.cstar_read_enable
        )
        self.assertEqual(
            os.environ.get("GOBEANSPROXY_TEST_CW") == "1",
            self.cstar_write_enable
        )

    @BaseTest.require_rw_enable(br=(True,), bw=(True,), cr=(False,), cw=(False,))
    def test_big_value(self):
        store = MCStore(self.proxy.addr)
        key = 'largekey'
        size = 10 * 1024 * 1024
        string_large = random_string(size // 10) * 10

        self.assertTrue(store.set(key, string_large))
        self.assertEqual(store.get(key), string_large)

        rsize = (((size + len(key) + 24) >> 8) + 1) << 8
        self.update_pos(rsize)

        self.assertEqual(self.get_meta(store, key), (1, 0, self.last_pos))

        self.assertTrue(store.set(key, 'aaa'))
        self.update_pos(256)
        self.assertEqual(self.get_meta(store, key), (2, 0, self.last_pos))

        self.checkCounterZero()
