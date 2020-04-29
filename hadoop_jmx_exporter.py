#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY

from cmd import utils
from cmd.utils import get_module_logger
from cmd.hdfs_namenode import NameNodeMetricCollector
from cmd.hdfs_datanode import DataNodeMetricCollector
from cmd.hdfs_journalnode import JournalNodeMetricCollector
from cmd.yarn_resourcemanager import ResourceManagerMetricCollector
from cmd.yarn_nodemanager import NodeManagerMetricCollector

logger = get_module_logger(__name__)


def register_prometheus(cluster, args):
    if args.nns is not None and len(args.nns) > 0:
        REGISTRY.register(NameNodeMetricCollector(cluster, args.nns))
    if args.dns is not None and len(args.dns) > 0:
        REGISTRY.register(DataNodeMetricCollector(cluster, args.dns))
    if args.jns is not None and len(args.jns) > 0:
        REGISTRY.register(JournalNodeMetricCollector(cluster, args.jns))
    if args.rms is not None and len(args.rms) > 0:
        REGISTRY.register(ResourceManagerMetricCollector(cluster, args.rms, args.queue))
    if args.nms is not None and len(args.nms) > 0:
        REGISTRY.register(NodeManagerMetricCollector(cluster, args.nms))

def main():
    args = utils.parse_args()
    host = args.host
    port = int(args.port)
    start_http_server(port, host)
    print "Listen at %s:%s" % (host, port)
    register_prometheus(args.cluster, args)
    while True:
        time.sleep(300)


if __name__ == "__main__":
    main()
