#!/usr/bin/env python
# encoding: utf-8

import os
import yaml
import copy
import errno
from os.path import join


gobeansdb_conf_tmpl = {
    'hstore': {
        'data': {
            'check_vhash': True,
            'datafile_max_str': '4000M',
            'flush_interval': 60,
            'flush_max_str': '100M',
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
            'tree_height': 7,
        },
        'local': {
            'homes': ['/var/lib/beansdb']
        }
    },
    'mc': {
        'body_big_str': '5M',
        'body_c_str': '4K',
        'body_max_str': '50M',
        'max_key_len': 250,
        'max_req': 16
    },
    'server': {
        'hostname': '127.0.0.1',
        'listen': '0.0.0.0',
        'logdir': '/var/log/gobeansdb',
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
            'buckets': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
        },
        {
            'addr': '127.0.0.1:7981',
            'buckets': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
        },
        {
            'addr': '127.0.0.1:7982',
            'buckets': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
        }
    ],
    'numbucket': 16
}

proxy_conf_tmpl = {
    'dstore': {
        'connect_timeout_ms': 300,
        'dial_fail_silence_ms': 5000,
        'max_free_conns_per_host': 20,
        'n': 1,
        'w': 1,
        'r': 1,
        'read_timeout_ms': 2000,
        'write_timeout_ms': 2000
    },
    'mc': {
        'body_big_str': '5M',
        'body_c_str': '4K',
        'body_max_str': '50M',
        'max_key_len': 250,
        'max_req': 16
    },
    'proxy': {
        'hostname': '127.0.0.1',
        'listen': '0.0.0.0',
        'logdir': './',
        'port': 7905,
        'threads': 8,
        'webport': 7908
    }
}


### 注意这里的端口号需要和 gobeansproxy/conf/route.yaml 的端口号一致

# (serverport, webport)
MAIN_PORT_PAIRS = [
    (7980, 7990),
    (7981, 7991),
    (7982, 7992),
]

BACKUP_PORT_PAIRS = [
    (7983, 7993),
]

PROXY_PORT_PAIRS = (7905, 7908)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--root-dir', help="root directory")
    args = parser.parse_args()
    gen_conf(os.path.abspath(args.root_dir))


def gen_conf(root_dir):
    ports = [x[0] for x in MAIN_PORT_PAIRS]
    backup_ports = [x[0] for x in BACKUP_PORT_PAIRS]
    route_conf = gen_route_conf(ports, backup_ports)


    ############# proxy
    # root_dir/proxy/conf/*.yaml
    # root_dir/proxy/*.log
    proxy_dir = gen_dir(root_dir, 'proxy')
    proxy_conf_dir = gen_dir(proxy_dir, 'conf')

    proxy_conf = gen_proxy_conf(proxy_dir,
                                PROXY_PORT_PAIRS[0],
                                PROXY_PORT_PAIRS[1])
    yaml_dump(proxy_conf, join(proxy_conf_dir, 'proxy.yaml'))
    yaml_dump(route_conf, join(proxy_conf_dir, 'route.yaml'))

    ############# server
    # root_dir/<serverport>/conf/*.yaml
    # root_dir/<serverport>/data/
    # root_dir/<serverport>/*.log
    for (port, webport) in (MAIN_PORT_PAIRS + BACKUP_PORT_PAIRS):
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
    tmpl['hstore']['local']['homes'] = [homedir]
    tmpl['server']['logdir'] = logdir
    tmpl['server']['port'] = port
    tmpl['server']['webport'] = webport
    return tmpl


def gen_route_conf(ports, backup_ports, numbucket=16):
    tmpl = copy.deepcopy(route_conf_tmpl)
    host = '127.0.0.1'
    buckets = ['%x' % i for i in range(numbucket)]
    tmpl['backup'] = ['%s:%s' % (host, p) for p in backup_ports]
    tmpl['main'] = [{'addr': '%s:%s' % (host, p), 'buckets': buckets}
                    for p in ports]
    return tmpl

def gen_proxy_conf(logdir, port, webport):
    tmpl = copy.deepcopy(proxy_conf_tmpl)
    tmpl['proxy']['logdir'] = logdir
    tmpl['proxy']['port'] = port
    tmpl['proxy']['webport'] = webport
    return tmpl


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


if __name__ == '__main__':
    main()