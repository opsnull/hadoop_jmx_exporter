#!/usr/bin/python
# -*- coding: utf-8 -*-

from prometheus_client.core import GaugeMetricFamily
from scraper import ScrapeMetrics

from utils import get_module_logger
from common import MetricCollector

logger = get_module_logger(__name__)


class TrinoCoordinatorMetricCollector(MetricCollector):

    def __init__(self, cluster, urls):
        MetricCollector.__init__(self, cluster, "trino", "coordinator")
        self.trino_coordinator_prefix = 'trino_coordinator'
        self.target = "-"
        self.urls = urls

        self.trino_coordinator_metrics = {}
        for i in range(len(self.file_list)):
            self.trino_coordinator_metrics.setdefault(self.file_list[i], {})

        self.scrape_metrics = ScrapeMetrics(urls)

    def collect(self):
        isSetup = False
        # isGetHost = False
        beans_list = self.scrape_metrics.scrape()
        for beans in beans_list:
            if not isSetup:
                self.setup_metrics_labels(beans)
                isSetup = True
            # if not isGetHost:
            for i in range(len(beans)):
                if 'java.lang:type=Runtime' in beans[i]['objectName']:
                    self.target = beans[i]['attributes'][0]['value'].split('@')[1]
                    # isGetHost = True
                    break
            self.get_metrics(beans)

        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            if service in self.trino_coordinator_metrics:
                for metric in self.trino_coordinator_metrics[service]:
                    yield self.trino_coordinator_metrics[service][metric]

    def setup_metrics_labels(self, beans):
        for i in range(len(beans)):
            if 'java.lang:type=Memory' in beans[i]['objectName']:
                self.setup_trino_coor_labels('Memory')
            if 'java.lang:type=Threading' in beans[i]['objectName']:
                self.setup_trino_coor_labels('Threading')
            if 'trino.execution:name=QueryManager' in beans[i]['objectName']:
                self.setup_trino_coor_labels('QueryManager')
            if 'trino.execution:name=SqlTaskManager' in beans[i]['objectName']:
                self.setup_trino_coor_labels('SqlTaskManager')
            if 'trino.failuredetector:name=HeartbeatFailureDetector' in beans[i]['objectName']:
                self.setup_trino_coor_labels('HeartbeatFailureDetector')
            if 'trino.memory:name=ClusterMemoryManager' in beans[i]['objectName']:
                self.setup_trino_coor_labels('ClusterMemoryManager')
            if 'trino.memory:type=ClusterMemoryPool,name=general' in beans[i]['objectName']:
                self.setup_trino_coor_labels('ClusterMemoryPool')

    def setup_trino_coor_labels(self, kind):
        label = ["cluster", "method", "_target"]
        name = "_".join([self.trino_coordinator_prefix, kind])
        description = "Trino Coordinator {0} metric.".format(kind)
        # 暂时没有细分，如果后面在kind内部继续划分key，可以用上
        key = kind
        self.trino_coordinator_metrics[kind][key] = GaugeMetricFamily(name, description, labels=label)

    def get_metrics(self, beans):
        for i in range(len(beans)):
            if 'java.lang:type=Memory' in beans[i]['objectName']:
                self.get_trino_coor_labels(beans[i], 'Memory')
            if 'java.lang:type=Threading' in beans[i]['objectName']:
                self.get_trino_coor_labels(beans[i], 'Threading')
            if 'trino.execution:name=QueryManager' in beans[i]['objectName']:
                self.get_trino_coor_labels(beans[i], 'QueryManager')
            if 'trino.execution:name=SqlTaskManager' in beans[i]['objectName']:
                self.get_trino_coor_labels(beans[i], 'SqlTaskManager')
            if 'trino.failuredetector:name=HeartbeatFailureDetector' in beans[i]['objectName']:
                self.get_trino_coor_labels(beans[i], 'HeartbeatFailureDetector')
            if 'trino.memory:name=ClusterMemoryManager' in beans[i]['objectName']:
                self.get_trino_coor_labels(beans[i], 'ClusterMemoryManager')
            if 'trino.memory:type=ClusterMemoryPool,name=general' in beans[i]['objectName']:
                self.get_trino_coor_labels(beans[i], 'ClusterMemoryPool')

    def get_trino_coor_labels(self, bean, kind):
        # type(bean) = dict
        for metric in self.metrics[kind]:
            value = 0
            for attr in bean['attributes']:
                key = kind
                method = metric.replace('.', '_').replace(':', '_').replace('-', '_')
                label = [self.cluster, method, self.target]
                if attr['name'] == metric:
                    if kind == 'Memory' and 'HeapMemoryUsage' in metric:
                        manu = 'used'
                        value = attr['value'][manu]
                    else:
                        value = attr['value']
                    break
            self.trino_coordinator_metrics[kind][key].add_metric(label, value)