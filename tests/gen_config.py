#!/usr/bin/env python
# encoding: utf-8

import os
import yaml
import copy
from os.path import join
from tests.utils import mkdir_p

gobeansdb_conf_tmpl = {
    'hstore': {
        'data': {
            'check_vhash': True,
            'datafile_max_str': '4000M',
            'flush_interval': 60,
            'flush_wake_str': '10M',
            'no_gc_days': 7,
        },
        'hint': {
            'hint_index_interval_str': '32K',
            'hint_merge_interval': 5,
            'hint_no_merged': True,
            'hint_split_cap_str': '1M',
        },
        'htree': {
            'tree_height': 3,
        },
        'local': {
            'home': '/var/lib/beansdb'
        }
    },
    'mc': {
        'body_big_str': '5M',
        'body_c_str': '0K',
        'flush_max_str': '100M',
        'body_max_str': '50M',
        'max_key_len': 250,
        'max_req': 16
    },
    'server': {
        'hostname': '127.0.0.1',
        'listen': '0.0.0.0',
        'errorlog': '/var/log/gobeansdb/error.log',
        'accesslog': '',
        'port': 7900,
        'threads': 4,
        'webport': 7903,
        'zk': 'NO'
    }
}

route_conf_tmpl = {
    'backup': ['127.0.0.1:7983'],
    'main': [
        {
            'addr': '127.0.0.1:7980',
            'buckets': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
                        'b', 'c', 'd', 'e', 'f']
        }, {
            'addr': '127.0.0.1:7981',
            'buckets': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
                        'b', 'c', 'd', 'e', 'f']
        }, {
            'addr': '127.0.0.1:7982',
            'buckets': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
                        'b', 'c', 'd', 'e', 'f']
        }
    ],
    'numbucket': 16
}

proxy_conf_tmpl = {
    'dstore': {
        'connect_timeout_ms': 300,
        'dial_fail_silence_ms': 5000,
        'max_free_conns_per_host': 20,
        'n': 3,
        'w': 2,
        'r': 1,
        'read_timeout_ms': 2000,
        'write_timeout_ms': 2000,
        'response_time_seconds': 10,
        'error_seconds': 10,
        'max_connect_errors': 3,
        'score_deviation': 10,
        'item_size_stats': 4096,
        'response_time_min': 4000,
        'enable_write': True,
        'enable_read': False,
    },
    'mc': {
        'body_big_str': '5M',
        'body_c_str': '0K',
        'body_max_str': '50M',
        'max_key_len': 250,
        'max_req': 16
    },
    'proxy': {
        'hostname': '127.0.0.1',
        'listen': '0.0.0.0',
        'errorlog': '/var/log/gobeansproxy/error.log',
        'accesslog': '/var/log/gobeansproxy/access.log',
        'staticdir': '/var/lib/gobeansproxy',
        'port': 7905,
        'threads': 8,
        'webport': 7908,
        'zkservers': ["zk1:2181"],
        'zkpath': "/gobeansproxy/test",
    },
    'cassandra': {
        'enable_read': True,
        'enable_write': True,
        'hosts': ["127.0.0.1:9042"],
        'default_key_space': 'doubandb',
        'default_table': 'kvstore',
        'timeout_sec': 5,
    },
}

### 注意这里的端口号需要和 gobeansproxy/conf/route.yaml 的端口号一致

# (serverport, webport)
MAIN_PORT_PAIRS = [(7980, 7990), (7981, 7991), (7982, 7992), ]

BACKUP_PORT_PAIRS = [(7983, 7993), ]

PROXY_PORT_PAIRS = (7905, 7908)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--root-dir', help="root directory")
    args = parser.parse_args()
    gen_conf(
        os.path.abspath(args.root_dir), MAIN_PORT_PAIRS, BACKUP_PORT_PAIRS,
        PROXY_PORT_PAIRS)


def gen_conf(root_dir,
             main_port_pairs=MAIN_PORT_PAIRS,
             backup_port_pairs=BACKUP_PORT_PAIRS,
             proxy_port_pairs=PROXY_PORT_PAIRS,
             bdb_read_enable=True, bdb_write_enable=True,
             cstar_read_enable=False, cstar_write_enable=False):
    ports = [x[0] for x in main_port_pairs]
    backup_ports = [x[0] for x in backup_port_pairs]
    route_conf = gen_route_conf(ports, backup_ports)

    ############# proxy
    # root_dir/proxy/conf/*.yaml
    # root_dir/proxy/*.log
    proxy_dir = gen_dir(root_dir, 'proxy')
    proxy_conf_dir = gen_dir(proxy_dir, 'conf')

    proxy_conf = gen_proxy_conf(
        proxy_dir, proxy_port_pairs[0], proxy_port_pairs[1],
        bdb_read_enable, bdb_write_enable,
        cstar_read_enable, cstar_write_enable
    )
    yaml_dump(proxy_conf, join(proxy_conf_dir, 'proxy.yaml'))
    yaml_dump(route_conf, join(proxy_conf_dir, 'route.yaml'))

    for (port, webport) in (MAIN_PORT_PAIRS + BACKUP_PORT_PAIRS):
        gen_gobeansdb_conf(root_dir, route_conf, port, webport)


def gen_gobeansdb_conf(root_dir, route_conf, port, webport):
    ############# server
    # root_dir/<serverport>/conf/*.yaml
    # root_dir/<serverport>/data/
    # root_dir/<serverport>/*.log
    server_dir = gen_dir(root_dir, str(port))
    server_conf_dir = gen_dir(server_dir, 'conf')
    server_data_dir = gen_dir(server_dir, 'data')

    server_conf = gen_server_conf(server_data_dir, server_dir, port, webport)
    yaml_dump(server_conf, join(server_conf_dir, 'global.yaml'))
    yaml_dump(route_conf, join(server_conf_dir, 'route.yaml'))


def gen_dir(*args):
    dir_ = join(*args)
    mkdir_p(dir_)
    return dir_


def yaml_dump(conf, filename):
    with open(filename, "w") as f:
        yaml.dump(conf, stream=f, indent=4, default_flow_style=False)


def gen_server_conf(homedir, logdir, port, webport):
    tmpl = copy.deepcopy(gobeansdb_conf_tmpl)
    tmpl['hstore']['local']['home'] = homedir
    tmpl['server']['errorlog'] = os.path.join(logdir, 'error.log')
    tmpl['server']['accesslog'] = os.path.join(logdir, 'access.log')
    tmpl['server']['port'] = port
    tmpl['server']['webport'] = webport
    return tmpl


def gen_route_conf(ports, backup_ports, numbucket=16):
    tmpl = copy.deepcopy(route_conf_tmpl)
    host = '127.0.0.1'
    buckets = ['%x' % i for i in range(numbucket)]
    tmpl['backup'] = ['%s:%s' % (host, p) for p in backup_ports]
    tmpl['main'] = [{'addr': '%s:%s' % (host, p),
                     'buckets': buckets} for p in ports]
    return tmpl


def gen_proxy_conf(
        logdir, port, webport,
        bdb_read_enable=True, bdb_write_enable=True,
        cstar_read_enable=False, cstar_write_enable=False):
    tmpl = copy.deepcopy(proxy_conf_tmpl)
    tmpl['proxy']['errorlog'] = os.path.join(logdir, 'error.log')
    tmpl['proxy']['accesslog'] = os.path.join(logdir, 'access.log')
    tmpl['proxy']['port'] = port
    tmpl['proxy']['webport'] = webport

    assert (bdb_read_enable or cstar_read_enable) \
        and (bdb_write_enable or cstar_write_enable), \
        'must enable at least one engine'
    
    tmpl['cassandra']['enable_read'] = cstar_read_enable
    tmpl['cassandra']['enable_write'] = cstar_write_enable
    tmpl['dstore']['enable_read'] = bdb_read_enable
    tmpl['dstore']['enable_write'] = bdb_write_enable
    return tmpl


if __name__ == '__main__':
    main()
