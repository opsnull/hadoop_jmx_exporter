#!/usr/bin/python
# -*- coding: utf-8 -*-

from prometheus_client.core import GaugeMetricFamily
from scraper import ScrapeMetrics

from utils import get_module_logger
from common import MetricCollector

logger = get_module_logger(__name__)


class HiveServerMetricCollector(MetricCollector):

    def __init__(self, cluster, urls):
        MetricCollector.__init__(self, cluster, "hive", "hiveserver2")
        self.target = "-"
        self.urls = urls

        self.hadoop_hiveserver2_metrics = {}
        for i in range(len(self.file_list)):
            self.hadoop_hiveserver2_metrics.setdefault(self.file_list[i], {})

        self.scrape_metrics = ScrapeMetrics(urls)

    def collect(self):
        isSetup = False
        beans_list = self.scrape_metrics.scrape()
        for beans in beans_list:
            if not isSetup:
                self.setup_metrics_labels(beans)
                isSetup = True
            for i in range(len(beans)):
                if 'tag.Hostname' in beans[i]:
                    self.target = beans[i]["tag.Hostname"]
                    break
            self.get_metrics(beans)

        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            if service in self.hadoop_hiveserver2_metrics:
                for metric in self.hadoop_hiveserver2_metrics[service]:
                    yield self.hadoop_hiveserver2_metrics[service][metric]

    def setup_metrics_labels(self, beans):
        # for i in range(len(beans)):
        #     if 'Hadoop:service=hiveserver2,name=hiveserver2' in beans[i]['name']:
        #         self.setup_hiveserver2_labels()
        self.setup_hiveserver2_labels()

    def setup_hiveserver2_labels(self):
        # for metric in self.metrics['HiveServer2']:
        label = ["cluster", "method", "_target"]
        key = "Hiveserver2"
        name = "_".join([self.prefix, key])
        description = "Hive Server2 metric."
        self.hadoop_hiveserver2_metrics['HiveServer2'][key] = GaugeMetricFamily(name, description, labels=label)

    def get_metrics(self, beans):
        for i in range(len(beans)):
            if 'Hadoop:service=hiveserver2,name=hiveserver2' in beans[i]['name']:
                self.get_hiveserver2_labels(beans[i])

    def get_hiveserver2_labels(self, bean):
        for metric in self.metrics['HiveServer2']:
            key = "Hiveserver2"
            method = metric.replace('.', '_').replace('-', '_')
            label = [self.cluster, method, self.target]
            self.hadoop_hiveserver2_metrics['HiveServer2'][key].add_metric(label, bean[metric] if metric in bean else 0)
