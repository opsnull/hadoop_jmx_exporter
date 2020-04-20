#!/usr/bin/python
# -*- coding: utf-8 -*-


import re
from prometheus_client.core import GaugeMetricFamily, HistogramMetricFamily

import utils
from utils import get_module_logger
from common import MetricCollector, common_metrics_collector

logger = get_module_logger(__name__)


class JournalNodeMetricCollector(MetricCollector):
    def __init__(self, cluster, url):
        MetricCollector.__init__(self, cluster, url, "hdfs", "journalnode")
        self._hadoop_journalnode_metrics = {}
        self._target = "-"
        for i in range(len(self._file_list)):
            self._hadoop_journalnode_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        try:
            beans = utils.get_metrics(self._url)
        except:
            logger.info("Can't scrape metrics from url: {0}".format(self._url))
            pass
        else:
            for i in range(len(beans)):
                if 'name=Journal-' in beans[i]['name']:
                    self._target = beans[i]["tag.Hostname"]
                    break

            # set up all metrics with labels and descriptions.
            self._setup_metrics_labels(beans)

            # add metric value to every metric.
            self._get_metrics(beans)

            # update namenode metrics with common metrics
            common_metrics = common_metrics_collector(self._cluster, beans, "hdfs", "journalnode", self._target)
            self._hadoop_journalnode_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_journalnode_metrics[service]:
                    yield self._hadoop_journalnode_metrics[service][metric]

    def _setup_journalnode_labels(self):
        a_60_latency_flag, a_300_latency_flag, a_3600_latency_flag = 1, 1, 1
        for metric in self._metrics['JournalNode']:
            label = ["cluster", "host", "_target"]
            if 'Syncs60s' in metric:
                if a_60_latency_flag:
                    a_60_latency_flag = 0
                    key = "Syncs60"
                    name = "_".join([self._prefix, 'sync60s_latency_microseconds'])
                    descriptions = "The percentile of sync latency in microseconds in 60s granularity"
                    self._hadoop_journalnode_metrics['JournalNode'][key] = HistogramMetricFamily(name, descriptions, labels=label)
                else:
                    continue
            elif 'Syncs300s' in metric:
                if a_300_latency_flag:
                    a_300_latency_flag = 0
                    key = "Syncs300"
                    name = "_".join([self._prefix, 'sync300s_latency_microseconds'])
                    descriptions = "The percentile of sync latency in microseconds in 300s granularity"
                    self._hadoop_journalnode_metrics['JournalNode'][key] = HistogramMetricFamily(name, descriptions, labels=label)
                else:
                    continue
            elif 'Syncs3600s' in metric:
                if a_3600_latency_flag:
                    a_3600_latency_flag = 0
                    key = "Syncs3600"
                    name = "_".join([self._prefix, 'sync3600s_latency_microseconds'])
                    descriptions = "The percentile of sync latency in microseconds in 3600s granularity"
                    self._hadoop_journalnode_metrics['JournalNode'][key] = HistogramMetricFamily(name, descriptions, labels=label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self._prefix, snake_case])
                self._hadoop_journalnode_metrics['JournalNode'][metric] = GaugeMetricFamily(name, self._metrics['JournalNode'][metric], labels=label)

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for i in range(len(beans)):
            # 格式类似于："name": "Hadoop:service=JournalNode,name=Journal-nameservice1"
            # nameservice1 为集群名称
            if 'name=Journal-' in beans[i]['name']:
                self._setup_journalnode_labels()

    def _get_metrics(self, beans):
        for i in range(len(beans)):
            if 'name=Journal-' in beans[i]['name']:
                if 'JournalNode' in self._metrics:
                    host = beans[i]['tag.Hostname']
                    label = [self._cluster, host, self._target]

                    a_60_sum, a_300_sum, a_3600_sum = 0.0, 0.0, 0.0
                    a_60_value, a_300_value, a_3600_value = [], [], []
                    a_60_percentile, a_300_percentile, a_3600_percentile = [], [], []

                    for metric in beans[i]:
                        if metric[0].isupper():
                            '''
                            different sync times corresponding to the same percentile
                            for instance:
                                sync = 60, percentile can be [50, 75, 95, 99]
                                sync = 300, percentile still can be [50, 75, 95, 99]
                            Therefore, here is the method to distinguish these metrics from each sync times.
                            '''
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
                                self._hadoop_journalnode_metrics['JournalNode'][key].add_metric(label, beans[i][metric])
                    a_60_bucket = zip(a_60_percentile, a_60_value)
                    a_300_bucket = zip(a_300_percentile, a_300_value)
                    a_3600_bucket = zip(a_3600_percentile, a_3600_value)
                    a_60_bucket.sort()
                    a_300_bucket.sort()
                    a_3600_bucket.sort()
                    a_60_bucket.append(("+Inf", a_60_count))
                    a_300_bucket.append(("+Inf", a_300_count))
                    a_3600_bucket.append(("+Inf", a_3600_count))
                    self._hadoop_journalnode_metrics['JournalNode']['Syncs60'].add_metric(label, buckets=a_60_bucket, sum_value=a_60_sum)
                    self._hadoop_journalnode_metrics['JournalNode']['Syncs300'].add_metric(label, buckets=a_300_bucket, sum_value=a_300_sum)
                    self._hadoop_journalnode_metrics['JournalNode']['Syncs3600'].add_metric(label, buckets=a_3600_bucket, sum_value=a_3600_sum)
