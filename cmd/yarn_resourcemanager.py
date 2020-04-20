#!/usr/bin/python
# -*- coding: utf-8 -*-

import yaml
import re
from prometheus_client.core import GaugeMetricFamily

import utils
from utils import get_module_logger
from common import MetricCollector, common_metrics_collector

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

    def __init__(self, cluster, url):
        MetricCollector.__init__(self, cluster, url, "yarn", "resourcemanager")
        self._hadoop_resourcemanager_metrics = {}
        self._target = "-"
        for i in range(len(self._file_list)):
            self._hadoop_resourcemanager_metrics.setdefault(
                self._file_list[i], {})

    def collect(self):
        try:
            beans = utils.get_metrics(self._url)
        except:
            logger.info("Can't scrape metrics from url: {0}".format(self._url))
            pass
        else:
            for i in range(len(beans)):
                if 'ClusterMetrics' in beans[i]['name']:
                    self._target = beans[i]["tag.Hostname"]
                    break
            # set up all metrics with labels and descriptions.
            self._setup_metrics_labels(beans)

            # add metric value to every metric.
            self._get_metrics(beans)

            # update namenode metrics with common metrics
            common_metrics = common_metrics_collector(self._cluster, beans, "yarn", "resourcemanager", self._target)
            self._hadoop_resourcemanager_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_resourcemanager_metrics[service]:
                    yield self._hadoop_resourcemanager_metrics[service][metric]

    def _setup_rmnminfo_labels(self):
        for metric in self._metrics['RMNMInfo']:
            label = ["cluster", "host", "version", "rack", "_target"]
            if 'NumContainers' in metric:
                name = "_".join([self._prefix, 'node_containers_total'])
            elif 'State' in metric:
                name = "_".join([self._prefix, 'node_state'])
            elif 'UsedMemoryMB' in metric:
                name = "_".join([self._prefix, 'node_memory_used_mb'])
            elif 'AvailableMemoryMB' in metric:
                name = "_".join([self._prefix, 'node_memory_available_mb'])
            else:
                pass
            self._hadoop_resourcemanager_metrics['RMNMInfo'][metric] = GaugeMetricFamily(name, self._metrics['RMNMInfo'][metric], labels=label)

    def _setup_queue_labels(self):
        running_flag, mb_flag, vcore_flag, container_flag, apps_flag = 1, 1, 1, 1, 1
        for metric in self._metrics['QueueMetrics']:
            label = ["cluster", "modeler_type", "queue", "user"]
            if "running_" in metric:
                if running_flag:
                    running_flag = 0
                    label.append("elapsed_time")
                    key = "running_app"
                    name = "_".join([self._prefix, "running_app_total"])
                    description = "Current number of running applications in each elapsed time ( < 60min, 60min < x < 300min, 300min < x < 1440min and x > 1440min )"
                else:
                    continue
            elif metric.endswith("VCores"):
                if vcore_flag:
                    vcore_flag = 0
                    label.append("status")
                    key = "vcore"
                    name = "_".join([self._prefix, "vcore_count"])
                    description = "Count of vcore"
                else:
                    continue
            elif metric.endswith("Containers"):
                if container_flag:
                    container_flag = 0
                    label.append("status")
                    key = "containers"
                    name = "_".join([self._prefix, "container_count"])
                    description = "Count of container"
                else:
                    continue
            elif metric.endswith("MB"):
                if mb_flag:
                    mb_flag = 0
                    label.append("status")
                    key = "memory"
                    name = "_".join([self._prefix, "memory_in_mb"])
                    description = "Memory in MB"
                else:
                    continue
            elif metric.startswith("Apps"):
                if apps_flag:
                    apps_flag = 0
                    label.append("status")
                    key = "apps"
                    name = "_".join([self._prefix, "application_count"])
                    description = "Count of application"
                else:
                    continue
            else:
                key = metric
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self._prefix, snake_case])
                description = self._metrics['QueueMetrics'][metric]
            label.append("_target")
            self._hadoop_resourcemanager_metrics['QueueMetrics'][key] = GaugeMetricFamily(name, description, labels=label)

    def _setup_cluster_labels(self):
        nm_flag, cm_num_flag, cm_avg_flag = 1, 1, 1
        for metric in self._metrics['ClusterMetrics']:
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
                description = self._metrics['ClusterMetrics'][metric]
                label = ["cluster"]
            label.append("_target")
            self._hadoop_resourcemanager_metrics['ClusterMetrics'][key] = GaugeMetricFamily("_".join([self._prefix, name]), description, labels=label)

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for i in range(len(beans)):
            if 'RMNMInfo' in beans[i]['name']:
                self._setup_rmnminfo_labels()
            if 'QueueMetrics' in self._metrics:
                self._setup_queue_labels()
            if 'ClusterMetrics' in self._metrics:
                self._setup_cluster_labels()

    def _get_rmnminfo_metrics(self, bean):
        for metric in self._metrics['RMNMInfo']:
            live_nm_list = yaml.safe_load(bean['LiveNodeManagers'])
            for j in range(len(live_nm_list)):
                host = live_nm_list[j]['HostName']
                version = live_nm_list[j]['NodeManagerVersion']
                rack = live_nm_list[j]['Rack']
                label = [self._cluster, host, version, rack, self._target]
                if 'State' == metric:
                    value = self.NODE_STATE[live_nm_list[j]['State']]
                else:
                    value = live_nm_list[j][metric] if metric in live_nm_list[j] else 0.0
                self._hadoop_resourcemanager_metrics['RMNMInfo'][metric].add_metric(label, value)

    def _get_queue_metrics(self, bean):
        for metric in self._metrics['QueueMetrics']:
            label = [self._cluster, bean.get("modelerType", "-"), bean.get("tag.Queue", "-"), bean.get("tag.User", "-")]
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
            label.append(self._target)
            self._hadoop_resourcemanager_metrics['QueueMetrics'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def _get_cluster_metrics(self, bean):
        for metric in self._metrics['ClusterMetrics']:
            label = [self._cluster]
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
            label.append(self._target)
            self._hadoop_resourcemanager_metrics['ClusterMetrics'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def _get_metrics(self, beans):
        for i in range(len(beans)):
            if 'RMNMInfo' in beans[i]['name']:
                self._get_rmnminfo_metrics(beans[i])
            if 'QueueMetrics' in beans[i]['name'] and 'root' == beans[i]['tag.Queue']:
                self._get_queue_metrics(beans[i])
            if 'ClusterMetrics' in beans[i]['name']:
                self._get_cluster_metrics(beans[i])
