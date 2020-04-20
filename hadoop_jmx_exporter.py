#!/usr/bin/python
# -*- coding: utf-8 -*-

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
    if args.namenode_url is not None:
        REGISTRY.register(NameNodeMetricCollector(cluster, args.namenode_url))
    if args.datanode_url is not None:
        REGISTRY.register(DataNodeMetricCollector(cluster, args.datanode_url))
    if args.journalnode_url is None:
        REGISTRY.register(JournalNodeMetricCollector(cluster, args.journalnode_url))
    if args.resourcemanager_url is not None:
        REGISTRY.register(ResourceManagerMetricCollector(cluster, args.resourcemanager_url))
    if args.nodemanager_url is not None:
        REGISTRY.register(NodeManagerMetricCollector(cluster, args.nodemanager_url))


def main():
    args = utils.parse_args()
    address = args.address
    port = int(args.port)
    start_http_server(port, address)
    print "Listen at %s:%s" % (address, port)
    register_prometheus(args.cluster, args)
    raw_input("hang")


if __name__ == "__main__":
    main()
