#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import socket
import requests
import argparse
import logging
import yaml


def get_module_logger(mod_name):
    '''
    define a common logger template to record log.
    @param mod_name log module name.
    @return logger.
    '''
    logger = logging.getLogger(mod_name)
    logger.setLevel(logging.DEBUG)

    # 设置日志文件handler，并设置记录级别
    path = os.path.dirname(os.path.abspath(__file__))
    par_path = os.path.dirname(path)
    fh = logging.FileHandler(os.path.join(par_path, "hadoop_jmx_exporter.log"))
    fh.setLevel(logging.INFO)

    # 设置终端输出handler，并设置记录级别
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)

    # 设置日志格式
    fmt = logging.Formatter(fmt='%(asctime)s %(filename)s[line:%(lineno)d]-[%(levelname)s]: %(message)s')
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)

    # 添加handler到logger对象
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


logger = get_module_logger(__name__)


def get_metrics(url):
    '''
    :param url: The jmx url, e.g. http://host1:50070/jmx,http://host1:8088/jmx, http://host2:19888/jmx...
    :return a dict of all metrics scraped in the jmx url.
    '''
    result = []
    try:
        s = requests.session()
        response = s.get(url, timeout=5)
    except Exception as e:
        logger.warning("error in func: get_metrics, error msg: %s" % e)
        result = []
    else:
        if response.status_code != requests.codes.ok:
            logger.warning("Get {0} failed, response code is: {1}.".format(url, response.status_code))
            result = []
        rlt = response.json()
        logger.debug(rlt)
        if rlt and "beans" in rlt:
            result = rlt['beans']
        else:
            logger.warning("No metrics get in the {0}.".format(url))
            result = []
    finally:
        s.close()
    return result


def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def get_hostname():
    '''
    get hostname via socket.
    @return a string of hostname
    '''
    try:
        host = socket.getfqdn()
    except Exception as e:
        logger.info("get hostname failed, error msg: {0}".format(e))
        return None
    else:
        return host


def read_json_file(path_name, file_name):
    '''
    read metric json files.
    '''
    path = os.path.dirname(os.path.realpath(__file__))
    metric_path = os.path.join(path, "../metrics", path_name)
    metric_name = "{0}.json".format(file_name)
    try:
        with open(os.path.join(metric_path, metric_name), 'r') as f:
            metrics = yaml.safe_load(f)
            return metrics
    except Exception as e:
        logger.info("read metrics json file failed, error msg is: %s" % e)
        return {}


def get_file_list(file_path_name):
    '''
    This function is to get all .json file name in the specified file_path_name.
    @param file_path: The file path name, e.g. namenode, ugi, resourcemanager ...
    @return a list of file name.
    '''
    path = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(path, "../metrics", file_path_name)
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
    parser.add_argument('-queue', required=False, metavar='queue_regexp', help='Regular expression of queue name. default: root.*', default='root.*')
    parser.add_argument('-nns', required=False, metavar='namenode_jmx_url', help='Hadoop hdfs namenode jmx metrics URL.', nargs="*")
    parser.add_argument('-dns', required=False, metavar='datanode_jmx_url', help='Hadoop datanode jmx metrics URL.', nargs="*")
    parser.add_argument('-rms', required=False, metavar='resourcemanager_jmx_url', help='Hadoop resourcemanager metrics jmx URL.', nargs="*")
    parser.add_argument('-nms', required=False, metavar='nodemanager_jmx_url', help='Hadoop nodemanager jmx metrics URL.', nargs="*")
    parser.add_argument('-jns', required=False, metavar='journalnode_jmx_url', help='Hadoop journalnode jmx metrics URL.', nargs="*")
    parser.add_argument('-host', required=False, metavar='ip_or_hostname', help='Listen on this address. default: 0.0.0.0', default='0.0.0.0')
    parser.add_argument('-port', required=False, metavar='port', type=int, help='Listen to this port. default: 6688', default=6688)
    parser.add_argument('-path', required=False, metavar='metrics_path', help='Path under which to expose metrics. default: /metrics', default='/metrics')
    return parser.parse_args()
