import os
import errno
import yaml
try:
    from yaml import Cloader as Loader
except ImportError:
    from yaml import Loader
import string
import socket
import random
import urllib.request, urllib.error, urllib.parse
import time
import shlex
import subprocess


def mkdir_p(path):
    "like `mkdir -p`"
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def random_string(n):
    s = string.ascii_letters
    result = ""
    for _ in range(n):
        result += random.choice(s)
    return result


def gethttp(addr, path):
    url = "http://%s/%s" % (addr, path)
    response = urllib.request.urlopen(url)
    return response.read()


def start_cmd(cmd):
    print("start", cmd)
    log_file = '/tmp/beansdb/log.txt'
    mkdir_p(os.path.dirname(log_file))
    with open(log_file, 'a') as f:
        p = subprocess.Popen(
            cmd if isinstance(cmd, (tuple, list)) else shlex.split(cmd),
            stderr=f,
        )
    time.sleep(0.2)
    if p.poll() is not None:
        raise Exception("cannot start %s" % (cmd))
    return p

def stop_cmd(popen):
    if popen.poll() is not None:
        return
    popen.terminate()
    popen.wait()


def get_server_addr(conf_dir, server_name):
    if server_name == 'gobeansdb':
        conf_file = 'global.yaml'
        key = 'server'
    elif server_name == 'gobeansproxy':
        conf_file = 'proxy.yaml'
        key = 'proxy'
    else:
        raise ValueError(server_name)
    conf = load_yaml(os.path.join(conf_dir, conf_file))
    port = conf[key]['port']
    webport = conf[key]['webport']
    host = conf[key]['hostname'] or socket.gethostname()

    def port_to_addr(port):
        return '%s:%s' % (host, port)

    return list(map(port_to_addr, [port, webport]))


def load_yaml(filepath):
    with open(filepath) as f:
        return yaml.load(f, Loader=Loader)
