#!/usr/bin/python
# -*- coding: utf-8 -*-


import re
from prometheus_client.core import GaugeMetricFamily

import utils
from utils import get_module_logger
from common import MetricCollector, CommonMetricCollector

logger = get_module_logger(__name__)


class NodeManagerMetricCollector(MetricCollector):

    def __init__(self, cluster, urls):
        MetricCollector.__init__(self, cluster, urls, "yarn", "nodemanager")
        self.target = "-"

        self.hadoop_nodemanager_metrics = {}
        for i in range(len(self.file_list)):
            self.hadoop_nodemanager_metrics.setdefault(self.file_list[i], {})

        self.common_metric_collector = CommonMetricCollector(cluster, "yarn", "nodemanager")

    def collect(self):
        for index, url in enumerate(self.urls):
            try:
                beans = utils.get_metrics(url)
            except Exception as e:
                logger.info("Can't scrape metrics from url: {0}, error: {1}".format(url, e))
            else:
                if index == 0:
                    self.common_metric_collector.setup_labels(beans)
                    self.setup_metrics_labels(beans)

                for i in range(len(beans)):
                    if 'tag.Hostname' in beans[i]:
                        self.target = beans[i]["tag.Hostname"]
                        break

                self.hadoop_nodemanager_metrics.update(self.common_metric_collector.get_metrics(beans, self.target))

                self.get_metrics(beans)

        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            for metric in self.hadoop_nodemanager_metrics[service]:
                yield self.hadoop_nodemanager_metrics[service][metric]

    def setup_metrics_labels(self, beans):
        for i in range(len(beans)):
            for service in self.metrics:
                if service in beans[i]['name']:
                    container_flag = 1
                    for metric in self.metrics[service]:
                        label = ["cluster", "host"]
                        if metric.startswith("Containers"):
                            if container_flag:
                                container_flag = 0
                                label.append("status")
                                key = "containers"
                                name = "_".join([self.prefix, "container_count"])
                                description = "Count of container"
                            else:
                                pass
                        else:
                            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                            name = "_".join([self.prefix, snake_case])
                            key = metric
                            description = self.metrics[service][metric]
                        label.append("target")
                        self.hadoop_nodemanager_metrics[service][key] = GaugeMetricFamily(name, description, labels=label)

    def get_metrics(self, beans):
        for i in range(len(beans)):
            for service in self.metrics:
                if service in beans[i]['name']:
                    for metric in beans[i]:
                        if metric in self.metrics[service]:
                            label = [self.cluster, self.target]
                            if metric.startswith("Containers"):
                                key = "containers"
                                label.append(metric.split("Containers")[1])
                            else:
                                key = metric
                            label.append(self.target)
                            value = beans[i][metric] if beans[i][metric] > 0 else 0  # incase vcore&memory<0
                            self.hadoop_nodemanager_metrics[service][key].add_metric(label, value)
