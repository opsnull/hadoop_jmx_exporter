#!/usr/bin/python
# -*- coding: utf-8 -*-

import yaml
import re
from prometheus_client.core import GaugeMetricFamily

import utils
from utils import get_module_logger
from common import MetricCollector, common_metrics_collector

logger = get_module_logger(__name__)


class DataNodeMetricCollector(MetricCollector):
    def __init__(self, cluster, url):
        MetricCollector.__init__(self, cluster, url, "hdfs", "datanode")
        self._hadoop_datanode_metrics = {}
        self._target = "-"
        for i in range(len(self._file_list)):
            self._hadoop_datanode_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        try:
            beans = utils.get_metrics(self._url)
        except Exception as e:
            logger.info("Can't scrape metrics from url: {0}, error: {1}".format(self._url, e))
        else:
            for i in range(len(beans)):
                if 'DataNodeActivity' in beans[i]['name']:
                    self._target = beans[i]['tag.Hostname']
                    break

            # set up all metrics with labels and descriptions.
            self._setup_metrics_labels(beans)

            # add metric value to every metric.
            self._get_metrics(beans)

            # update namenode metrics with common metrics
            common_metrics = common_metrics_collector(self._cluster, beans, "hdfs", "datanode", self._target)
            self._hadoop_datanode_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_datanode_metrics[service]:
                    yield self._hadoop_datanode_metrics[service][metric]

    def _setup_dninfo_labels(self):
        for metric in self._metrics['DataNodeInfo']:
            if 'VolumeInfo' in metric:
                label = ["cluster", "version", "path", "state"]
                name = "_".join([self._prefix, 'volume_state'])
            else:
                label = ["cluster", "version"]
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self._prefix, snake_case])
            label.append("_target")
            self._hadoop_datanode_metrics['DataNodeInfo'][metric] = GaugeMetricFamily(name, self._metrics['DataNodeInfo'][metric], labels=label)

    def _setup_dnactivity_labels(self):
        block_flag, client_flag = 1, 1
        for metric in self._metrics['DataNodeActivity']:
            # TODO: 判断以 Block 开头的关键字。排查 AvgTime
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
                descriptions = self._metrics['DataNodeActivity'][metric]
            label.append("_target")
            self._hadoop_datanode_metrics['DataNodeActivity'][key] = GaugeMetricFamily("_".join([self._prefix, name]), descriptions, labels=label)

    def _setup_fsdatasetstate_labels(self):
        for metric in self._metrics['FSDatasetState']:
            label = ['cluster', 'host', "_target"]
            if "Num" in metric:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric.split("Num")[1]).lower()
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            name = "_".join([self._prefix, snake_case])
            self._hadoop_datanode_metrics['FSDatasetState'][metric] = GaugeMetricFamily(name, self._metrics['FSDatasetState'][metric], labels=label)

    def _setup_metrics_labels(self, beans):
        for i in range(len(beans)):
            if 'DataNodeInfo' in beans[i]['name']:
                self._setup_dninfo_labels()
            if 'DataNodeActivity' in beans[i]['name']:
                self._setup_dnactivity_labels()
            if 'FSDatasetState' in beans[i]['name']:
                self._setup_fsdatasetstate_labels()

    def _get_dninfo_metrics(self, bean):
        for metric in self._metrics['DataNodeInfo']:
            version = bean['Version']
            if 'VolumeInfo' in metric:
                if 'VolumeInfo' in bean:
                    volume_info_dict = yaml.safe_load(bean['VolumeInfo'])
                    for k, v in volume_info_dict.items():
                        path = k
                        for key, val in v.items():
                            state = key
                            label = [self._cluster, version, path, state, self._target]
                            value = val
                            self._hadoop_datanode_metrics['DataNodeInfo'][metric].add_metric(label, value)
                else:
                    continue
            else:
                label = [self._cluster, version, self._target]
                value = bean[metric]
                self._hadoop_datanode_metrics['DataNodeInfo'][metric].add_metric(label, value)

    def _get_dnactivity_metrics(self, bean):
        for metric in self._metrics['DataNodeActivity']:
            host = bean['tag.Hostname']
            label = [self._cluster, host]
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
            label.append(self._target)
            self._hadoop_datanode_metrics['DataNodeActivity'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def _get_fsdatasetstate_metrics(self, bean):
        for metric in self._metrics['FSDatasetState']:
            label = [self._cluster, self._target, self._target]
            self._hadoop_datanode_metrics['FSDatasetState'][metric].add_metric(
                label, bean[metric] if metric in bean else 0)

    def _get_metrics(self, beans):
        for i in range(len(beans)):
            if 'DataNodeInfo' in beans[i]['name']:
                self._get_dninfo_metrics(beans[i])
            if 'DataNodeActivity' in beans[i]['name']:
                self._get_dnactivity_metrics(beans[i])
            if 'FSDatasetState' in beans[i]['name']:
                self._get_fsdatasetstate_metrics(beans[i])
