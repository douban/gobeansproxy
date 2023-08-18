import os
import pytest
import requests
import yaml

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


from .dbclient import MCStore as store

store_addr = os.getenv("GOBEANSPROXY_ADDR")
store_api = os.getenv("GOBEANSPROXY_WEB")
store_proxy_cfg = os.getenv("GOBEANSPROXY_PROXY_CFG") or \
    '/home/project/.doubanpde/scripts/bdb/gobeansproxy/prefix-switch-cfg/conf/proxy.yaml'

p_status_brw = 'br1w1cr0w0'
p_status_brw_cw = 'br1w1cr0w1'
p_status_bw_crw = 'br0w1cr1w1'
p_status_crw = 'br0w0cr1w1'

order_of_status = {
    p_status_brw: 0,
    p_status_brw_cw: 1,
    p_status_bw_crw: 2,
    p_status_crw: 3,
}


class TestSwitchStorage:

    def setup_method(self):
        self.client = store(store_addr or "127.0.0.1:47907")
        self.prefix = "/__test_proxy/"
        self.prefix_wont_switch = "/__test_proxy_no_switch/"
        self.key_max = 100
        self.web_addr = store_api or "http://localhost:47910/cstar-cfg-reload?config=prefixStorageSwitcher"
        self.web_req = requests.Session()
        self.store_proxy_cfg_backup = store_proxy_cfg + '.backup'
        # copy cfg bak
        with open(store_proxy_cfg, 'r+') as f:
            with open(self.store_proxy_cfg_backup, 'w') as b:
                b.write(f.read())
        self.status = p_status_brw
        self.switch_store(p_status_brw)

    def format_key(self, k):
        return f"{self.prefix}{k}"

    def teardown_method(self):
        self.web_req.close()
        with open(self.store_proxy_cfg_backup, 'r+') as f:
            with open(store_proxy_cfg, 'w') as o:
                o.write(f.read())

    @pytest.mark.parametrize("test_kv", [
        (1, 1),
        ("str", "str"),
        ("list", ["0", 1, 2]),
        ("dict", {"1": 1, "2": 2, "3": 3}),
        ("中文", "中文str"),
        ("bytes", b'abcde'),
        ("nesting", [{"abc中文": ["1", "2", "fff"]}]),
    ])
    def test_curd_value(self, test_kv):
        k, v = test_kv
        key = self.format_key(k)
        assert self.client.set(key, v)
        assert self.client.get(key) == v
        assert self.client.delete(key)
        assert self.client.get(key) is None

    @pytest.mark.parametrize("test_kvs", [
        (
            (1, 1),
            ("str", "str"),
            ("list", ["0", 1, 2]),
            ("dict", {"1": 1, "2": 2, "3": 3}),
            ("中文", "中文str"),
            ("bytes", b'abcde'),
            ("nesting", [{"abc中文": ["1", "2", "fff"]}]),
            ("bool", True),
        ),
    ])
    def test_getm_value(self, test_kvs):
        getm_prefix = '__test_proxy_getm/'
        r = {f'{getm_prefix}{k}': v for k, v in test_kvs}
        assert len(r) == len(test_kvs)
        keys = list(r.keys())

        for k, v in r.items():
            assert self.client.set(k, v)
        
        result = self.client.get_multi(keys)
        assert len(keys) == len(result)
        for k, v in result.items():
            assert r[k] == v
            assert self.client.delete(k)

    def trigger_reload(self):
        resp = self.web_req.post(self.web_addr)
        resp.raise_for_status()
        assert resp.json()['message'] == "success"

    def switch_store(self, switch_to):
        assert switch_to in (p_status_brw, p_status_brw_cw,
                             p_status_bw_crw, p_status_crw)
        if self.status == switch_to:
            return

        with open(store_proxy_cfg, 'r+') as f:
            data = load(f, Loader=Loader)
            data['cassandra']['switch_to_keyprefixes'] = {
                switch_to: [self.prefix]
            }

            f.seek(0, 0)
            f.truncate()

            f.write(dump(data, Dumper=Dumper))
        self.trigger_reload()
        self.status = switch_to

    def test_switch_store(self):

        switch_to = [
            # bdb -> cassandra
            (
                p_status_brw_cw,
                p_status_bw_crw,
                p_status_crw,
                p_status_brw
            ),

            # bdb -> cassandra dual write -> bdb -> cassandra
            (
                p_status_brw_cw,
                p_status_bw_crw,
                p_status_brw_cw,
                p_status_bw_crw,
                p_status_crw,
                p_status_brw
            ),
        ]

        key = self.format_key('switch_test')
        value = 'value'

        no_switch_key = f'{self.prefix_wont_switch}notme'
        no_switch_value = "static"

        assert self.client.set(key, value)
        assert self.client.set(no_switch_key, no_switch_value)

        for stages in switch_to:
            last_stage = None
            for idx, stage in enumerate(stages):
                last_stage = self.status
                self.switch_store(stage)

                # ensure we can still get values
                # when  change from crw -> other br status this is not going to equal
                if stage in (p_status_brw, p_status_brw_cw) and last_stage == p_status_crw:
                    assert self.client.get(key) != value, f'stages: {stages} -> stage: {stage} error'
                else:
                    assert self.client.get(key) == value, f'stages: {stages} -> stage: {stage} error'
                    assert self.client.get(no_switch_key) == no_switch_value, f'stages: {stages} -> stage: {stage} error'
                
                # ensure we can set to new value
                value = f'value_on_{stage}'
                assert self.client.set(key, value), f'stages: {stages} -> stage: {stage} error'
                assert self.client.get(key) == value, f'stages: {stages} -> stage: {stage} error'
                no_switch_value = f'static_on_{stage}'
                assert self.client.set(no_switch_key, no_switch_value), f'stages: {stages} -> stage: {stage} error'
                assert self.client.get(no_switch_key) == no_switch_value, f'stages: {stages} -> stage: {stage} error'

                # ensure we can delete value
                assert self.client.delete(key), f'stages: {stages} -> stage: {stage} error'
                assert self.client.set(key, value), f'stages: {stages} -> stage: {stage} error'
                assert self.client.get(key) == value, f'stages: {stages} -> stage: {stage} error'

        self.switch_store(p_status_brw)
        assert self.client.delete(key), f'stages: {stages} -> stage: {stage} error'
        assert self.client.delete(no_switch_key), f'stages: {stages} -> stage: {stage} error'
