#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import yaml
import re
from prometheus_client.core import GaugeMetricFamily

from utils import get_module_logger
from cmd.common import MetricCollector, CommonMetricCollector
from scraper import ScrapeMetrics

logger = get_module_logger(__name__)


class DataNodeMetricCollector(MetricCollector):
    def __init__(self, cluster, nnc):
        MetricCollector.__init__(self, cluster, "hdfs", "datanode")
        self.target = "-"
        self.nnc = nnc

        self.hadoop_datanode_metrics = {}
        for i in range(len(self.file_list)):
            self.hadoop_datanode_metrics.setdefault(self.file_list[i], {})

        self.common_metric_collector = CommonMetricCollector(cluster, "hdfs", "datanode")

    def collect(self):
        isSetup = False
        if self.nnc.dns == "":
            return
        beans_list = ScrapeMetrics(self.nnc.dns).scrape()
        for beans in beans_list:
            if not isSetup:
                self.common_metric_collector.setup_labels(beans)
                self.setup_metrics_labels(beans)
                isSetup = True
            for i in range(len(beans)):
                if 'tag.Hostname' in beans[i]:
                    self.target = beans[i]["tag.Hostname"]
                    break
            self.hadoop_datanode_metrics.update(self.common_metric_collector.get_metrics(beans, self.target))
            self.get_metrics(beans)

        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            if service in self.hadoop_datanode_metrics:
                for metric in self.hadoop_datanode_metrics[service]:
                    yield self.hadoop_datanode_metrics[service][metric]

    def setup_dninfo_labels(self):
        for metric in self.metrics['DataNodeInfo']:
            if 'VolumeInfo' in metric:
                label = ["cluster", "version", "path", "state"]
                name = "_".join([self.prefix, 'volume_state'])
            else:
                label = ["cluster", "version"]
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self.prefix, snake_case])
            label.append("_target")
            self.hadoop_datanode_metrics['DataNodeInfo'][metric] = GaugeMetricFamily(name, self.metrics['DataNodeInfo'][metric], labels=label)

    def setup_dnactivity_labels(self):
        block_flag, client_flag = 1, 1
        for metric in self.metrics['DataNodeActivity']:
            if 'Blocks' in metric:
                if block_flag:
                    label = ['cluster', 'host', 'oper']
                    key = "Blocks"
                    name = "block_operations_total"
                    descriptions = "Total number of blocks in different oprations"
                    block_flag = 0
                else:
                    continue
            elif 'Client' in metric:
                if client_flag:
                    label = ['cluster', 'host', 'oper', 'client']
                    key = "Client"
                    name = "from_client_total"
                    descriptions = "Total number of each operations from different client"
                    client_flag = 0
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ['cluster', 'host']
                key = metric
                name = snake_case
                descriptions = self.metrics['DataNodeActivity'][metric]
            label.append("_target")
            self.hadoop_datanode_metrics['DataNodeActivity'][key] = GaugeMetricFamily("_".join([self.prefix, name]), descriptions, labels=label)

    def setup_fsdatasetstate_labels(self):
        for metric in self.metrics['FSDatasetState']:
            label = ['cluster', 'host', "_target"]
            if "Num" in metric:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric.split("Num")[1]).lower()
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            name = "_".join([self.prefix, snake_case])
            self.hadoop_datanode_metrics['FSDatasetState'][metric] = GaugeMetricFamily(name, self.metrics['FSDatasetState'][metric], labels=label)

    def setup_metrics_labels(self, beans):
        for i in range(len(beans)):
            if 'DataNodeInfo' in beans[i]['name']:
                self.setup_dninfo_labels()
            if 'DataNodeActivity' in beans[i]['name']:
                self.setup_dnactivity_labels()
            if 'FSDatasetState' in beans[i]['name']:
                self.setup_fsdatasetstate_labels()

    def get_dninfo_metrics(self, bean):
        for metric in self.metrics['DataNodeInfo']:
            version = bean['Version']
            if 'VolumeInfo' in metric:
                if 'VolumeInfo' in bean:
                    volume_info_dict = yaml.safe_load(bean['VolumeInfo'])
                    for k, v in volume_info_dict.items():
                        path = k
                        for key, val in v.items():
                            if key != "storageType":
                                state = key
                                label = [self.cluster, version, path, state, self.target]
                                value = val
                                self.hadoop_datanode_metrics['DataNodeInfo'][metric].add_metric(label, value)
                else:
                    continue
            else:
                label = [self.cluster, version, self.target]
                value = bean[metric]
                self.hadoop_datanode_metrics['DataNodeInfo'][metric].add_metric(label, value)

    def get_dnactivity_metrics(self, bean):
        for metric in self.metrics['DataNodeActivity']:
            host = bean['tag.Hostname']
            label = [self.cluster, host]
            if 'Blocks' in metric:
                oper = metric.split("Blocks")[1]
                label.append(oper)
                key = "Blocks"
            elif 'Client' in metric:
                oper = metric.split("Client")[0].split("From")[0]
                client = metric.split("Client")[0].split("From")[1]
                label.extend([oper, client])
                key = "Client"
            else:
                key = metric
            label.append(self.target)
            self.hadoop_datanode_metrics['DataNodeActivity'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def get_fsdatasetstate_metrics(self, bean):
        for metric in self.metrics['FSDatasetState']:
            label = [self.cluster, self.target, self.target]
            self.hadoop_datanode_metrics['FSDatasetState'][metric].add_metric(
                label, bean[metric] if metric in bean else 0)

    def get_metrics(self, beans):
        for i in range(len(beans)):
            if 'DataNodeInfo' in beans[i]['name']:
                self.get_dninfo_metrics(beans[i])
            if 'DataNodeActivity' in beans[i]['name']:
                self.get_dnactivity_metrics(beans[i])
            if 'FSDatasetState' in beans[i]['name']:
                self.get_fsdatasetstate_metrics(beans[i])
