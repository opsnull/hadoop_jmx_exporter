#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import argparse
import logging
import yaml

verify = True
trust_env = True


def get_module_logger(mod_name):
    logger = logging.getLogger(mod_name)
    logger.setLevel(logging.DEBUG)

    path = os.path.dirname(os.path.abspath(__file__))
    par_path = os.path.dirname(path)
    fh = logging.FileHandler(os.path.join(par_path, "hadoop_jmx_exporter.log"))
    fh.setLevel(logging.INFO)

    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)

    fmt = logging.Formatter(fmt='%(asctime)s %(filename)s[line:%(lineno)d]-[%(levelname)s]: %(message)s')
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


logger = get_module_logger(__name__)

def read_json_file(path_name, file_name):
    path = os.path.dirname(os.path.realpath(__file__))
    metric_path = os.path.join(path, "metrics", path_name)
    metric_name = "{0}.json".format(file_name)
    try:
        with open(os.path.join(metric_path, metric_name), 'r') as f:
            metrics = yaml.safe_load(f)
            return metrics
    except Exception as e:
        logger.info("read metrics json file failed, error msg is: %s" % e)
        return {}


def get_file_list(file_path_name):
    path = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(path, "metrics", file_path_name)
    try:
        files = os.listdir(json_path)
    except OSError:
        logger.info("No such file or directory: '%s'" % json_path)
        return []
    else:
        rlt = []
        for i in range(len(files)):
            rlt.append(files[i].split(".json")[0])
        return rlt


def parse_args():
    parser = argparse.ArgumentParser(description='hadoop jmx metric prometheus exporter')
    parser.add_argument('-cluster', required=True, metavar='cluster_name', help='Hadoop cluster name (maybe HA name)')
    parser.add_argument('-queue', required=False, metavar='yarn_queue_regexp', help='Regular expression of queue name. default: root.*', default='root.*')
    parser.add_argument('-nns', required=False, metavar='namenode_jmx_url', help='Hadoop hdfs namenode jmx metrics URL.', nargs="*")
    parser.add_argument('-rms', required=False, metavar='resourcemanager_jmx_url', help='Hadoop resourcemanager metrics jmx URL.', nargs="*")
    parser.add_argument('-jns', required=False, metavar='journalnode_jmx_url', help='Hadoop journalnode jmx metrics URL.', nargs="*")
    parser.add_argument('-host', required=False, metavar='host', help='Listen on this address. default: 0.0.0.0', default='0.0.0.0')
    parser.add_argument('-port', required=False, metavar='port', type=int, help='Listen to this port. default: 6688', default=6688)
    parser.add_argument('-verify', required=False, metavar='verify', help='Scrape veirify switch. default: True', default='True')
    parser.add_argument('-trust', required=False, metavar='trust', help='Scrape trust env switch. default: True', default='True')
    args = parser.parse_args()
    global verify, trust_env
    verify = args.verify == 'True'
    trust_env = args.trust == 'True'
    return args
