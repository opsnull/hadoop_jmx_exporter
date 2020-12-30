#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import re
from prometheus_client.core import GaugeMetricFamily, HistogramMetricFamily

from utils import get_module_logger
from common import MetricCollector, CommonMetricCollector
from scraper import ScrapeMetrics

logger = get_module_logger(__name__)


class JournalNodeMetricCollector(MetricCollector):
    def __init__(self, cluster, urls):
        MetricCollector.__init__(self, cluster, "hdfs", "journalnode")
        self.target = "-"
        self.urls = urls

        self.hadoop_journalnode_metrics = {}
        for i in range(len(self.file_list)):
            self.hadoop_journalnode_metrics.setdefault(self.file_list[i], {})

        self.common_metric_collector = CommonMetricCollector(cluster, "hdfs", "journalnode")

        self.scrape_metrics = ScrapeMetrics(urls)

    def collect(self):
        isSetup = False
        beans_list = self.scrape_metrics.scrape()
        for beans in beans_list:
            if not isSetup:
                self.common_metric_collector.setup_labels(beans)
                self.setup_metrics_labels(beans)
                isSetup = True
            for i in range(len(beans)):
                if 'tag.Hostname' in beans[i]:
                    self.target = beans[i]["tag.Hostname"]
                    break
            self.hadoop_journalnode_metrics.update(self.common_metric_collector.get_metrics(beans, self.target))
            self.get_metrics(beans)

        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            if service in self.hadoop_journalnode_metrics:
                for metric in self.hadoop_journalnode_metrics[service]:
                    yield self.hadoop_journalnode_metrics[service][metric]

    def setup_journalnode_labels(self):
        a_60_latency_flag, a_300_latency_flag, a_3600_latency_flag = 1, 1, 1
        for metric in self.metrics['JournalNode']:
            label = ["cluster", "host", "_target"]
            if 'Syncs60s' in metric:
                if a_60_latency_flag:
                    a_60_latency_flag = 0
                    key = "Syncs60"
                    name = "_".join([self.prefix, 'sync60s_latency_microseconds'])
                    descriptions = "The percentile of sync latency in microseconds in 60s granularity"
                    self.hadoop_journalnode_metrics['JournalNode'][key] = HistogramMetricFamily(name, descriptions, labels=label)
                else:
                    continue
            elif 'Syncs300s' in metric:
                if a_300_latency_flag:
                    a_300_latency_flag = 0
                    key = "Syncs300"
                    name = "_".join([self.prefix, 'sync300s_latency_microseconds'])
                    descriptions = "The percentile of sync latency in microseconds in 300s granularity"
                    self.hadoop_journalnode_metrics['JournalNode'][key] = HistogramMetricFamily(name, descriptions, labels=label)
                else:
                    continue
            elif 'Syncs3600s' in metric:
                if a_3600_latency_flag:
                    a_3600_latency_flag = 0
                    key = "Syncs3600"
                    name = "_".join([self.prefix, 'sync3600s_latency_microseconds'])
                    descriptions = "The percentile of sync latency in microseconds in 3600s granularity"
                    self.hadoop_journalnode_metrics['JournalNode'][key] = HistogramMetricFamily(name, descriptions, labels=label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self.prefix, snake_case])
                self.hadoop_journalnode_metrics['JournalNode'][metric] = GaugeMetricFamily(name, self.metrics['JournalNode'][metric], labels=label)

    def setup_metrics_labels(self, beans):
        for i in range(len(beans)):
            if 'name=Journal-' in beans[i]['name']:
                self.setup_journalnode_labels()

    def get_metrics(self, beans):
        for i in range(len(beans)):
            if 'name=Journal-' in beans[i]['name'] and 'JournalNode' in self.metrics:
                host = beans[i]['tag.Hostname']
                label = [self.cluster, host, self.target]

                a_60_sum, a_300_sum, a_3600_sum = 0.0, 0.0, 0.0
                a_60_value, a_300_value, a_3600_value = [], [], []
                a_60_percentile, a_300_percentile, a_3600_percentile = [], [], []

                for metric in beans[i]:
                    if not metric[0].isupper():
                        continue
                    if "Syncs60s" in metric:
                        if 'NumOps' in metric:
                            a_60_count = beans[i][metric]
                        else:
                            tmp = metric.split("thPercentileLatencyMicros")[0].split("Syncs")[1].split("s")
                            a_60_percentile.append(str(float(tmp[1]) / 100.0))
                            a_60_value.append(beans[i][metric])
                            a_60_sum += beans[i][metric]
                    elif 'Syncs300' in metric:
                        if 'NumOps' in metric:
                            a_300_count = beans[i][metric]
                        else:
                            tmp = metric.split("thPercentileLatencyMicros")[0].split("Syncs")[1].split("s")
                            a_300_percentile.append(str(float(tmp[1]) / 100.0))
                            a_300_value.append(beans[i][metric])
                            a_300_sum += beans[i][metric]
                    elif 'Syncs3600' in metric:
                        if 'NumOps' in metric:
                            a_3600_count = beans[i][metric]
                        else:
                            tmp = metric.split("thPercentileLatencyMicros")[0].split("Syncs")[1].split("s")
                            a_3600_percentile.append(str(float(tmp[1]) / 100.0))
                            a_3600_value.append(beans[i][metric])
                            a_3600_sum += beans[i][metric]
                    else:
                        key = metric
                        self.hadoop_journalnode_metrics['JournalNode'][key].add_metric(label, beans[i][metric])
                a_60_bucket = zip(a_60_percentile, a_60_value)
                a_300_bucket = zip(a_300_percentile, a_300_value)
                a_3600_bucket = zip(a_3600_percentile, a_3600_value)
                a_60_bucket.sort()
                a_300_bucket.sort()
                a_3600_bucket.sort()
                a_60_bucket.append(("+Inf", a_60_count))
                a_300_bucket.append(("+Inf", a_300_count))
                a_3600_bucket.append(("+Inf", a_3600_count))
                self.hadoop_journalnode_metrics['JournalNode']['Syncs60'].add_metric(label, buckets=a_60_bucket, sum_value=a_60_sum)
                self.hadoop_journalnode_metrics['JournalNode']['Syncs300'].add_metric(label, buckets=a_300_bucket, sum_value=a_300_sum)
                self.hadoop_journalnode_metrics['JournalNode']['Syncs3600'].add_metric(label, buckets=a_3600_bucket, sum_value=a_3600_sum)
