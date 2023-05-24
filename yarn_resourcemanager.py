#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import yaml
import re
from prometheus_client.core import GaugeMetricFamily

from utils import get_module_logger
from common import MetricCollector, CommonMetricCollector
from scraper import ScrapeMetrics

logger = get_module_logger(__name__)


class ResourceManagerMetricCollector(MetricCollector):

    NODE_STATE = {
        'NEW': 1,
        'RUNNING': 2,
        'UNHEALTHY': 3,
        'DECOMMISSIONED': 4,
        'LOST': 5,
        'REBOOTED': 6,
    }

    def __init__(self, cluster, urls, queue_regexp):
        MetricCollector.__init__(self, cluster, "yarn", "resourcemanager")
        self.target = "-"
        self.queue_regexp = queue_regexp
        self.nms = set()

        self.hadoop_resourcemanager_metrics = {}
        for i in range(len(self.file_list)):
            self.hadoop_resourcemanager_metrics.setdefault(self.file_list[i], {})

        self.common_metric_collector = CommonMetricCollector(cluster, "yarn", "resourcemanager")

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
            self.hadoop_resourcemanager_metrics.update(self.common_metric_collector.get_metrics(beans, self.target))
            self.get_metrics(beans)

        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            if service in self.hadoop_resourcemanager_metrics:
                for metric in self.hadoop_resourcemanager_metrics[service]:
                    yield self.hadoop_resourcemanager_metrics[service][metric]

    def setup_rmnminfo_labels(self):
        for metric in self.metrics['RMNMInfo']:
            label = ["cluster", "host", "version", "rack", "_target"]
            if 'NumContainers' in metric:
                name = "_".join([self.prefix, 'node_containers_total'])
            elif 'State' in metric:
                name = "_".join([self.prefix, 'node_state'])
            elif 'UsedMemoryMB' in metric:
                name = "_".join([self.prefix, 'node_memory_used_mb'])
            elif 'AvailableMemoryMB' in metric:
                name = "_".join([self.prefix, 'node_memory_available_mb'])
            else:
                continue
            self.hadoop_resourcemanager_metrics['RMNMInfo'][metric] = GaugeMetricFamily(name, self.metrics['RMNMInfo'][metric], labels=label)

    def setup_queue_labels(self):
        running_flag, mb_flag, vcore_flag, container_flag, apps_flag = 1, 1, 1, 1, 1
        for metric in self.metrics['QueueMetrics']:
            label = ["cluster", "modeler_type", "queue", "user"]
            if "running_" in metric:
                if running_flag:
                    running_flag = 0
                    label.append("elapsed_time")
                    key = "running_app"
                    name = "_".join([self.prefix, "running_app_total"])
                    description = "Current number of running applications in each elapsed time ( < 60min, 60min < x < 300min, 300min < x < 1440min and x > 1440min )"
                else:
                    continue
            elif metric.endswith("VCores"):
                if vcore_flag:
                    vcore_flag = 0
                    label.append("status")
                    key = "vcore"
                    name = "_".join([self.prefix, "vcore_count"])
                    description = "Count of vcore"
                else:
                    continue
            elif metric.endswith("Containers"):
                if container_flag:
                    container_flag = 0
                    label.append("status")
                    key = "containers"
                    name = "_".join([self.prefix, "container_count"])
                    description = "Count of container"
                else:
                    continue
            elif metric.endswith("MB"):
                if mb_flag:
                    mb_flag = 0
                    label.append("status")
                    key = "memory"
                    name = "_".join([self.prefix, "memory_in_mb"])
                    description = "Memory in MB"
                else:
                    continue
            elif metric.startswith("Apps"):
                if apps_flag:
                    apps_flag = 0
                    label.append("status")
                    key = "apps"
                    name = "_".join([self.prefix, "application_count"])
                    description = "Count of application"
                else:
                    continue
            else:
                key = metric
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self.prefix, snake_case])
                description = self.metrics['QueueMetrics'][metric]
            label.append("_target")
            self.hadoop_resourcemanager_metrics['QueueMetrics'][key] = GaugeMetricFamily(name, description, labels=label)

    def setup_cluster_labels(self):
        nm_flag, cm_num_flag, cm_avg_flag = 1, 1, 1
        for metric in self.metrics['ClusterMetrics']:
            if "NMs" in metric:
                if nm_flag:
                    nm_flag = 0
                    label = ["cluster", "status"]
                    key = "NMs"
                    name = "nodemanager_total"
                    description = "Current number of NodeManagers in each status"
                else:
                    continue
            elif "NumOps" in metric:
                if cm_num_flag:
                    cm_num_flag = 0
                    label = ["cluster", "oper"]
                    key = "NumOps"
                    name = "ams_total"
                    description = "Total number of Applications Masters in each operation"
                else:
                    continue
            elif "AvgTime" in metric:
                if cm_avg_flag:
                    cm_avg_flag = 0
                    label = ["cluster", "oper"]
                    key = "AvgTime"
                    name = "average_time_milliseconds"
                    description = "Average time in milliseconds AM spends in each operation"
                else:
                    continue
            else:
                key = metric
                name = metric
                description = self.metrics['ClusterMetrics'][metric]
                label = ["cluster"]
            label.append("_target")
            self.hadoop_resourcemanager_metrics['ClusterMetrics'][key] = GaugeMetricFamily("_".join([self.prefix, name]), description, labels=label)

    def setup_metrics_labels(self, beans):
        for i in range(len(beans)):
            if 'RMNMInfo' in beans[i]['name']:
                self.setup_rmnminfo_labels()
            if 'QueueMetrics' in self.metrics:
                self.setup_queue_labels()
            if 'ClusterMetrics' in self.metrics:
                self.setup_cluster_labels()

    def get_rmnminfo_metrics(self, bean):
        for metric in self.metrics['RMNMInfo']:
            nms = set()
            live_nm_list = yaml.safe_load(bean['LiveNodeManagers'])
            for j in range(len(live_nm_list)):
                nms.add("http://"+live_nm_list[j]["NodeHTTPAddress"]+"/jmx")
                host = live_nm_list[j]['HostName']
                version = live_nm_list[j]['NodeManagerVersion']
                rack = live_nm_list[j]['Rack']
                label = [self.cluster, host, version, rack, self.target]
                if 'State' == metric:
                    value = self.NODE_STATE[live_nm_list[j]['State']]
                else:
                    value = live_nm_list[j][metric] if metric in live_nm_list[j] else 0.0
                self.hadoop_resourcemanager_metrics['RMNMInfo'][metric].add_metric(label, value)
            self.nms = nms

    def get_queue_metrics(self, bean):
        for metric in self.metrics['QueueMetrics']:
            label = [self.cluster, bean.get("modelerType", "-"), bean.get("tag.Queue", "-"), bean.get("tag.User", "-")]
            if "running_0" in metric:
                key = "running_app"
                label.append("0to60")
            elif "running_60" in metric:
                key = "running_app"
                label.append("60to300")
            elif "running_300" in metric:
                key = "running_app"
                label.append("300to1440")
            elif "running_1440" in metric:
                key = "running_app"
                label.append("1440up")
            elif metric.endswith("VCores"):
                label.append(metric.split("VCores")[0])
                key = "vcore"
            elif metric.endswith("Containers"):
                label.append(metric.split("Containers")[0])
                key = "containers"
            elif metric.endswith("MB"):
                label.append(metric.split("MB")[0])
                key = "memory"
            elif metric.startswith("Apps"):
                label.append(metric.split("Apps")[1])
                key = "apps"
            else:
                key = metric
            label.append(self.target)
            self.hadoop_resourcemanager_metrics['QueueMetrics'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def get_cluster_metrics(self, bean):
        for metric in self.metrics['ClusterMetrics']:
            label = [self.cluster]
            if "NMs" in metric:
                label.append(metric.split('NMs')[0].split('Num')[1])
                key = "NMs"
            elif "NumOps" in metric:
                key = "NumOps"
                label.append(metric.split("DelayNumOps")[0].split('AM')[1])
            elif "AvgTime" in metric:
                key = "AvgTime"
                label.append(metric.split("DelayAvgTime")[0].split('AM')[1])
            else:
                continue
            label.append(self.target)
            self.hadoop_resourcemanager_metrics['ClusterMetrics'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def get_metrics(self, beans):
        for i in range(len(beans)):
            if 'RMNMInfo' in beans[i]['name']:
                self.get_rmnminfo_metrics(beans[i])
            if 'QueueMetrics' in beans[i]['name'] and re.match(self.queue_regexp, beans[i]['tag.Queue']):
                self.get_queue_metrics(beans[i])
            if 'ClusterMetrics' in beans[i]['name']:
                self.get_cluster_metrics(beans[i])
