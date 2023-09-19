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
            if 'io.airlift.stats:name=GcMonitor' in beans[i]['objectName']:
                self.setup_trino_coor_labels('GcMonitor')
            if 'java.lang:name=G1 Young Generation,type=GarbageCollector' in beans[i]['objectName']:
                # self.setup_trino_coor_labels('G1YoungGarbageCollector')
                self.setup_young_gc_labels();
            if 'java.lang:name=G1 Old Generation,type=GarbageCollector' in beans[i]['objectName']:
                # self.setup_trino_coor_labels('G1OldGarbageCollector')
                self.setup_old_gc_labels();

    def setup_trino_coor_labels(self, kind):
        label = ["cluster", "method", "_target"]
        name = "_".join([self.trino_coordinator_prefix, kind])
        description = "Trino Coordinator {0} metric.".format(kind)
        # 暂时没有细分，如果后面在kind内部继续划分key，可以用上
        key = kind
        self.trino_coordinator_metrics[kind][key] = GaugeMetricFamily(name, description, labels=label)

    def setup_young_gc_labels(self):
        kind = 'G1YoungGarbageCollector'
        label = ["cluster", "method", "_target"]
        name = "_".join([self.trino_coordinator_prefix, kind])

        young_before, young_after = 1, 1
        for metric in self.metrics[kind]:
            if metric.split('.')[0] == 'LastGcInfo':
                key = metric.split('.')[1]
                if key == 'memoryUsageBeforeGc' and young_before:
                    description = 'Trino node memory usage metric before Young GC.'
                    self.trino_coordinator_metrics[kind][key] = GaugeMetricFamily(name, description, labels=label)
                    young_before = 0
                elif key == 'memoryUsageAfterGc' and young_after:
                    description = 'Trino node memory usage metric after Young GC.'
                    self.trino_coordinator_metrics[kind][key] = GaugeMetricFamily(name, description, labels=label)
                    young_after = 0
            else:
                key = kind
                description = 'Trino node Old Young metric'
                self.trino_coordinator_metrics[kind][key] = GaugeMetricFamily(name, description, labels=label)

    def setup_old_gc_labels(self):
        kind = 'G1OldGarbageCollector'
        label = ["cluster", "method", "_target"]
        name = "_".join([self.trino_coordinator_prefix, kind])

        young_before, young_after = 1, 1
        for metric in self.metrics[kind]:
            if metric.split('.')[0] == 'LastGcInfo':
                key = metric.split('.')[1]
                if key == 'memoryUsageBeforeGc' and young_before:
                    description = 'Trino node memory usage metric before Old GC.'
                    self.trino_coordinator_metrics[kind][key] = GaugeMetricFamily(name, description, labels=label)
                    young_before = 0
                elif key == 'memoryUsageAfterGc' and young_after:
                    description = 'Trino node memory usage metric after Old GC.'
                    self.trino_coordinator_metrics[kind][key] = GaugeMetricFamily(name, description, labels=label)
                    young_after = 0
            else:
                key = kind
                description = 'Trino node Old GC metric'
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
            if 'io.airlift.stats:name=GcMonitor' in beans[i]['objectName']:
                self.get_trino_coor_labels(beans[i], 'GcMonitor')
            if 'java.lang:name=G1 Young Generation,type=GarbageCollector' in beans[i]['objectName']:
                # self.get_trino_coor_labels(beans[i], 'G1YoungGarbageCollector')
                self.get_young_gc_labels(beans[i]);
            if 'java.lang:name=G1 Old Generation,type=GarbageCollector' in beans[i]['objectName']:
                # self.get_trino_coor_labels(beans[i], 'G1OldGarbageCollector')
                self.get_old_gc_labels(beans[i]);

    def get_trino_coor_labels(self, bean, kind):
        # type(bean) = dict
        for metric in self.metrics[kind]:
            key = kind
            label = [self.cluster, '', self.target]
            value = 0
            for attr in bean['attributes']:
                # type(attr) = dict
                method = metric.replace('.', '_').replace(':', '_').replace('-', '_')
                label = [self.cluster, method, self.target]
                if attr['name'] == metric:
                    if kind == 'Memory' and 'HeapMemoryUsage' in metric:
                        manu = 'used'
                        value = attr['value'][manu]
                    else:
                        value = attr['value']
                    break
            if not self.trino_coordinator_metrics[kind].has_key(key):
                self.setup_trino_coor_labels(kind)
            self.trino_coordinator_metrics[kind][key].add_metric(label, value)

    def get_young_gc_labels(self, bean):
        kind = 'G1YoungGarbageCollector'
        for metric in self.metrics[kind]:
            method = metric.replace('.', '_').replace(':', '_').replace('-', '_')
            label = [self.cluster, method, self.target]
            value = 0
            for attr in bean['attributes']:
                if attr['name'] == 'LastGcInfo' and len(metric.split('.')) > 2 and metric.split('.')[0] == 'LastGcInfo':
                    if 'value' in attr:
                        key = metric.split('.')[1]
                        for vl in attr['value'][key]:
                            if vl['key'] == metric.split('.')[2]:
                                manu = 'used'
                                value = vl['value'][manu]
                                self.trino_coordinator_metrics[kind][key].add_metric(label, value)
                elif attr['name'] == metric:
                    key = kind
                    value = attr['value']
                    self.trino_coordinator_metrics[kind][key].add_metric(label, value)

    def get_old_gc_labels(self, bean):
        kind = 'G1OldGarbageCollector'
        for metric in self.metrics[kind]:
            method = metric.replace('.', '_').replace(':', '_').replace('-', '_')
            label = [self.cluster, method, self.target]
            value = 0
            for attr in bean['attributes']:
                if attr['name'] == 'LastGcInfo' and len(metric.split('.')) > 2 and metric.split('.')[0] == 'LastGcInfo':
                    if 'value' in attr:
                        key = metric.split('.')[1]
                        for vl in attr['value'][key]:
                            if vl['key'] == metric.split('.')[2]:
                                manu = 'used'
                                value = vl['value'][manu]
                                self.trino_coordinator_metrics[kind][key].add_metric(label, value)
                elif attr['name'] == metric:
                    key = kind
                    value = attr['value']
                    self.trino_coordinator_metrics[kind][key].add_metric(label, value)