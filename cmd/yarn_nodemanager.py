#!/usr/bin/python
# -*- coding: utf-8 -*-


import re
from prometheus_client.core import GaugeMetricFamily

import utils
from utils import get_module_logger
from common import MetricCollector, common_metrics_collector

logger = get_module_logger(__name__)


class NodeManagerMetricCollector(MetricCollector):

    def __init__(self, cluster, url):
        MetricCollector.__init__(self, cluster, url, "yarn", "nodemanager")
        self._hadoop_nodemanager_metrics = {}
        self._target = "-"
        for i in range(len(self._file_list)):
            self._hadoop_nodemanager_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        try:
            beans = utils.get_metrics(self._url)
        except:
            logger.info("Can't scrape metrics from url: {0}".format(self._url))
            pass
        else:
            for i in range(len(beans)):
                if 'tag.Hostname' in beans[i]:
                    self._target = beans[i]['tag.Hostname']
                    break
            # set up all metrics with labels and descriptions.
            self._setup_metrics_labels(beans)

            # add metric value to every metric.
            self._get_metrics(beans)

            # update namenode metrics with common metrics
            common_metrics = common_metrics_collector(self._cluster, beans, "yarn", "nodemanager", self._target)
            self._hadoop_nodemanager_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_nodemanager_metrics[service]:
                    yield self._hadoop_nodemanager_metrics[service][metric]

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for i in range(len(beans)):
            for service in self._metrics:
                if service in beans[i]['name']:
                    container_flag = 1
                    for metric in self._metrics[service]:
                        label = ["cluster", "host"]
                        if metric.startswith("Containers"):
                            if container_flag:
                                container_flag = 0
                                label.append("status")
                                key = "containers"
                                name = "_".join([self._prefix, "container_count"])
                                description = "Count of container"
                            else:
                                pass
                        else:
                            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                            name = "_".join([self._prefix, snake_case])
                            key = metric
                            description = self._metrics[service][metric]
                        label.append("target")
                        self._hadoop_nodemanager_metrics[service][key] = GaugeMetricFamily(name,description,  labels=label)

    def _get_metrics(self, beans):
        for i in range(len(beans)):
            for service in self._metrics:
                if service in beans[i]['name']:
                    for metric in beans[i]:
                        if metric in self._metrics[service]:
                            label = [self._cluster, self._target]
                            if metric.startswith("Containers"):
                                key = "containers"
                                label.append(metric.split("Containers")[1])
                            else:
                                key = metric
                            label.append(self._target)
                            value = beans[i][metric] if beans[i][metric] > 0 else 0 # incase vcore&memory<0
                            self._hadoop_nodemanager_metrics[service][key].add_metric(label, value)
