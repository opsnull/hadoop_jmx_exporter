#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import re
from prometheus_client.core import GaugeMetricFamily

import utils

logger = utils.get_module_logger(__name__)


class MetricCollector(object):
    def __init__(self, cluster, component, service):
        self.cluster = cluster
        self.component = component
        self.prefix = 'hadoop_{0}_{1}'.format(component, service)

        self.file_list = utils.get_file_list(service)
        self.metrics = {}
        for i in range(len(self.file_list)):
            self.metrics.setdefault(self.file_list[i], utils.read_json_file(service, self.file_list[i]))

        common_file = utils.get_file_list("common")
        if component == "hdfs" or component == "yarn":
            self.merge_list = self.file_list + common_file
        else:
            self.merge_list = self.file_list

    def collect(self):
        pass

    def _setup_metrics_labels(self):
        pass

    def _get_metrics(self, metrics):
        pass


class CommonMetricCollector():
    def __init__(self, cluster, component, service):
        self.cluster = cluster
        self.componet = component
        self.service = service
        self.prefix = 'hadoop_{0}_{1}'.format(component, service)
        self.common_metrics = {}
        self.tmp_metrics = {}
        file_list = utils.get_file_list("common")
        for i in range(len(file_list)):
            self.common_metrics.setdefault(file_list[i], {})
            self.tmp_metrics.setdefault(file_list[i], utils.read_json_file("common", file_list[i]))

    def setup_labels(self, beans):
        for i in range(len(beans)):
            if 'name=JvmMetrics' in beans[i]['name']:
                self.setup_jvm_labels()
            if 'OperatingSystem' in beans[i]['name']:
                self.setup_os_labels()
            if 'RpcActivity' in beans[i]['name']:
                self.setup_rpc_labels()
            if 'RpcDetailedActivity' in beans[i]['name']:
                self.setup_rpc_detailed_labels()
            if 'UgiMetrics' in beans[i]['name']:
                self.setup_ugi_labels()
            if 'MetricsSystem' in beans[i]['name'] and "sub=Stats" in beans[i]['name']:
                self.setup_metric_system_labels()
            if 'Runtime' in beans[i]['name']:
                self.setup_runtime_labels()

    def get_metrics(self, beans, target):
        self.target = target
        for i in range(len(beans)):
            if 'name=JvmMetrics' in beans[i]['name']:
                self.get_jvm_metrics(beans[i])
            if 'OperatingSystem' in beans[i]['name']:
                self.get_os_metrics(beans[i])
            if 'RpcActivity' in beans[i]['name']:
                self.get_rpc_metrics(beans[i])
            if 'RpcDetailedActivity' in beans[i]['name']:
                self.get_rpc_detailed_metrics(beans[i])
            if 'UgiMetrics' in beans[i]['name']:
                self.get_ugi_metrics(beans[i])
            if 'MetricsSystem' in beans[i]['name'] and "sub=Stats" in beans[i]['name']:
                self.get_metric_system_metrics(beans[i])
            if 'Runtime' in beans[i]['name']:
                self.get_runtime_metrics(beans[i])
        return self.common_metrics

    def setup_jvm_labels(self):
        for metric in self.tmp_metrics["JvmMetrics"]:
            snake_case = "_".join(["jvm", re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()])
            if 'Mem' in metric:
                name = "".join([snake_case, "ebibytes"])
                label = ["cluster", "mode"]
                if "Used" in metric:
                    key = "jvm_mem_used_mebibytes"
                    descriptions = "Current memory used in mebibytes."
                elif "Committed" in metric:
                    key = "jvm_mem_committed_mebibytes"
                    descriptions = "Current memory committed in mebibytes."
                elif "Max" in metric:
                    key = "jvm_mem_max_mebibytes"
                    descriptions = "Current max memory in mebibytes."
                else:
                    key = name
                    label = ["cluster"]
                    descriptions = self.tmp_metrics['JvmMetrics'][metric]
            elif 'Gc' in metric:
                label = ["cluster", "type"]
                if "GcCount" in metric:
                    key = "jvm_gc_count"
                    descriptions = "GC count of each type GC."
                elif "GcTimeMillis" in metric:
                    key = "jvm_gc_time_milliseconds"
                    descriptions = "Each type GC time in milliseconds."
                elif "ThresholdExceeded" in metric:
                    key = "jvm_gc_exceeded_threshold_total"
                    descriptions = "Number of times that the GC threshold is exceeded."
                else:
                    key = snake_case
                    label = ["cluster"]
                    descriptions = self.tmp_metrics['JvmMetrics'][metric]
            elif 'Threads' in metric:
                label = ["cluster", "state"]
                key = "jvm_threads_state_total"
                descriptions = "Current number of different threads."
            elif 'Log' in metric:
                label = ["cluster", "level"]
                key = "jvm_log_level_total"
                descriptions = "Total number of each level logs."
            else:
                label = ["cluster"]
                key = snake_case
                descriptions = self.tmp_metrics['JvmMetrics'][metric]
            label.append("_target")
            self.common_metrics['JvmMetrics'][key] = GaugeMetricFamily("_".join([self.prefix, key]), descriptions,
                                                                       labels=label)

    def setup_os_labels(self):
        for metric in self.tmp_metrics['OperatingSystem']:
            label = ["cluster", "_target"]
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            name = "_".join([self.prefix, snake_case])
            self.common_metrics['OperatingSystem'][metric] = GaugeMetricFamily(name,
                                                                               self.tmp_metrics['OperatingSystem'][
                                                                                   metric], labels=label)

    def setup_rpc_labels(self):
        num_rpc_flag, avg_rpc_flag = 1, 1
        for metric in self.tmp_metrics["RpcActivity"]:
            snake_case = "_".join(["rpc", re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()])
            if 'Rpc' in metric:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            label = ["cluster", "tag"]
            if "NumOps" in metric:
                if num_rpc_flag:
                    key = "MethodNumOps"
                    label.extend(["method", "_target"])
                    name = "_".join([self.prefix, "rpc_method_called_total"])
                    description = "Total number of the times the method is called."
                    self.common_metrics['RpcActivity'][key] = GaugeMetricFamily(name, description, labels=label)
                    num_rpc_flag = 0
                else:
                    continue
            elif "AvgTime" in metric:
                if avg_rpc_flag:
                    key = "MethodAvgTime"
                    label.extend(["method", "_target"])
                    name = "_".join([self.prefix, "rpc_method_avg_time_milliseconds"])
                    descrption = "Average turn around time of the method in milliseconds."
                    self.common_metrics['RpcActivity'][key] = GaugeMetricFamily(name, descrption, labels=label)
                    avg_rpc_flag = 0
                else:
                    continue
            else:
                key = metric
                label.append("_target")
                name = "_".join([self.prefix, snake_case])
                self.common_metrics['RpcActivity'][key] = GaugeMetricFamily(name,
                                                                            self.tmp_metrics['RpcActivity'][metric],
                                                                            labels=label)

    def setup_rpc_detailed_labels(self):
        for metric in self.tmp_metrics['RpcDetailedActivity']:
            label = ["cluster", "tag", "method", "_target"]
            if "NumOps" in metric:
                key = "NumOps"
                name = "_".join([self.prefix, 'rpc_detailed_method_called_total'])
            elif "AvgTime" in metric:
                key = "AvgTime"
                name = "_".join([self.prefix, 'rpc_detailed_method_avg_time_milliseconds'])
            else:
                continue
            self.common_metrics['RpcDetailedActivity'][key] = GaugeMetricFamily(name,
                                                                                self.tmp_metrics['RpcDetailedActivity'][
                                                                                    metric], labels=label)
        return self.common_metrics

    def setup_ugi_labels(self):
        ugi_num_flag, ugi_avg_flag = 1, 1
        for metric in self.tmp_metrics['UgiMetrics']:
            label = ["cluster"]
            if 'NumOps' in metric:
                if ugi_num_flag:
                    key = 'NumOps'
                    label.extend(["method", "state", "_target"])
                    ugi_num_flag = 0
                    name = "_".join([self.prefix, 'ugi_method_called_total'])
                    description = "Total number of the times the method is called."
                    self.common_metrics['UgiMetrics'][key] = GaugeMetricFamily(name, description, labels=label)
                else:
                    continue
            elif 'AvgTime' in metric:
                if ugi_avg_flag:
                    key = 'AvgTime'
                    label.extend(["method", "state", "_target"])
                    ugi_avg_flag = 0
                    name = "_".join([self.prefix, 'ugi_method_avg_time_milliseconds'])
                    description = "Average turn around time of the method in milliseconds."
                    self.common_metrics['UgiMetrics'][key] = GaugeMetricFamily(name, description, labels=label)
                else:
                    continue
            else:
                label.append("_target")
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self.prefix, 'ugi', snake_case])
                self.common_metrics['UgiMetrics'][metric] = GaugeMetricFamily(name,
                                                                              self.tmp_metrics['UgiMetrics'][metric],
                                                                              labels=label)

    def setup_metric_system_labels(self):
        metric_num_flag, metric_avg_flag = 1, 1
        for metric in self.tmp_metrics['MetricsSystem']:
            label = ["cluster"]
            if 'NumOps' in metric:
                if metric_num_flag:
                    key = 'NumOps'
                    label.extend(["oper", "_target"])
                    metric_num_flag = 0
                    name = "_".join([self.prefix, 'metricssystem_operations_total'])
                    self.common_metrics['MetricsSystem'][key] = GaugeMetricFamily(name, "Total number of operations",
                                                                                  labels=label)
                else:
                    continue
            elif 'AvgTime' in metric:
                if metric_avg_flag:
                    key = 'AvgTime'
                    label.extend(["oper", "_target"])
                    metric_avg_flag = 0
                    name = "_".join([self.prefix, 'metricssystem_method_avg_time_milliseconds'])
                    description = "Average turn around time of the operations in milliseconds."
                    self.common_metrics['MetricsSystem'][key] = GaugeMetricFamily(name, description, labels=label)
                else:
                    continue
            else:
                label.append("_target")
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self.prefix, 'metricssystem', snake_case])
                self.common_metrics['MetricsSystem'][metric] = GaugeMetricFamily(name,
                                                                                 self.tmp_metrics['MetricsSystem'][
                                                                                     metric], labels=label)

    def setup_runtime_labels(self):
        for metric in self.tmp_metrics['Runtime']:
            label = ["cluster", "host", "_target"]
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            name = "_".join([self.prefix, snake_case, "milliseconds"])
            self.common_metrics['Runtime'][metric] = GaugeMetricFamily(name, self.tmp_metrics['Runtime'][metric],
                                                                       labels=label)

    def get_jvm_metrics(self, bean):
        for metric in self.tmp_metrics['JvmMetrics']:
            name = "_".join(["jvm", re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()])
            if 'Mem' in metric:
                if "Used" in metric:
                    key = "jvm_mem_used_mebibytes"
                    mode = metric.split("Used")[0].split("Mem")[1]
                    label = [self.cluster, mode]
                elif "Committed" in metric:
                    key = "jvm_mem_committed_mebibytes"
                    mode = metric.split("Committed")[0].split("Mem")[1]
                    label = [self.cluster, mode]
                elif "Max" in metric:
                    key = "jvm_mem_max_mebibytes"
                    if "Heap" in metric:
                        mode = metric.split("Max")[0].split("Mem")[1]
                    else:
                        mode = "max"
                    label = [self.cluster, mode]
                else:
                    key = "".join([name, 'ebibytes'])
                    label = [self.cluster]
            elif 'Gc' in metric:
                if "GcCount" in metric:
                    key = "jvm_gc_count"
                    if "GcCount" == metric:
                        typo = "total"
                    else:
                        typo = metric.split("GcCount")[1]
                    label = [self.cluster, typo]
                elif "GcTimeMillis" in metric:
                    key = "jvm_gc_time_milliseconds"
                    if "GcTimeMillis" == metric:
                        typo = "total"
                    else:
                        typo = metric.split("GcTimeMillis")[1]
                    label = [self.cluster, typo]
                elif "ThresholdExceeded" in metric:
                    key = "jvm_gc_exceeded_threshold_total"
                    typo = metric.split("ThresholdExceeded")[
                        0].split("GcNum")[1]
                    label = [self.cluster, typo]
                else:
                    key = name
                    label = [self.cluster]
            elif 'Threads' in metric:
                key = "jvm_threads_state_total"
                state = metric.split("Threads")[1]
                label = [self.cluster, state]
            elif 'Log' in metric:
                key = "jvm_log_level_total"
                level = metric.split("Log")[1]
                label = [self.cluster, level]
            else:
                key = name
                label = [self.cluster]
            label.append(self.target)
            self.common_metrics['JvmMetrics'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def get_os_metrics(self, bean):
        for metric in self.tmp_metrics['OperatingSystem']:
            label = [self.cluster]
            label.append(self.target)
            self.common_metrics['OperatingSystem'][metric].add_metric(label, bean[metric] if metric in bean else 0)

    def get_rpc_metrics(self, bean):
        rpc_tag = bean['tag.port']
        for metric in self.tmp_metrics['RpcActivity']:
            if "NumOps" in metric:
                method = metric.split('NumOps')[0]
                label = [self.cluster, rpc_tag, method]
                key = "MethodNumOps"
            elif "AvgTime" in metric:
                method = metric.split('AvgTime')[0]
                label = [self.cluster, rpc_tag, method]
                key = "MethodAvgTime"
            else:
                label = [self.cluster, rpc_tag]
                key = metric
            label.append(self.target)
            self.common_metrics['RpcActivity'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def get_rpc_detailed_metrics(self, bean):
        detail_tag = bean['tag.port']
        for metric in bean:
            if metric[0].isupper():
                if "NumOps" in metric:
                    key = "NumOps"
                    method = metric.split('NumOps')[0]
                elif "AvgTime" in metric:
                    key = "AvgTime"
                    method = metric.split("AvgTime")[0]
                else:
                    continue
                label = [self.cluster, detail_tag, method, self.target]
                self.common_metrics['RpcDetailedActivity'][key].add_metric(label, bean[metric])

    def get_ugi_metrics(self, bean):
        for metric in self.tmp_metrics['UgiMetrics']:
            if 'NumOps' in metric:
                key = 'NumOps'
                if 'Login' in metric:
                    method = 'Login'
                    state = metric.split('Login')[1].split('NumOps')[0]
                    label = [self.cluster, method, state]
                else:
                    method = metric.split('NumOps')[0]
                    label = [self.cluster, method, "-"]
            elif 'AvgTime' in metric:
                key = 'AvgTime'
                if 'Login' in metric:
                    method = 'Login'
                    state = metric.split('Login')[1].split('AvgTime')[0]
                    label = [self.cluster, method, state]
                else:
                    method = metric.split('AvgTime')[0]
                    label = [self.cluster, method, "-"]
            else:
                key = metric
                label = [self.cluster]
            label.append(self.target)
            self.common_metrics['UgiMetrics'][key].add_metric(label,
                                                              bean[metric] if metric in bean and bean[metric] else 0)

    def get_metric_system_metrics(self, bean):
        for metric in self.tmp_metrics['MetricsSystem']:
            if 'NumOps' in metric:
                key = 'NumOps'
                oper = metric.split('NumOps')[0]
                label = [self.cluster, oper]
            elif 'AvgTime' in metric:
                key = 'AvgTime'
                oper = metric.split('AvgTime')[0]
                label = [self.cluster, oper]
            else:
                key = metric
                label = [self.cluster]
            label.append(self.target)
            self.common_metrics['MetricsSystem'][key].add_metric(label,
                                                                 bean[metric] if metric in bean and bean[metric] else 0)

    def get_runtime_metrics(self, bean):
        for metric in self.tmp_metrics['Runtime']:
            label = [self.cluster, bean['Name'].split("@")[1], self.target]
            self.common_metrics['Runtime'][metric].add_metric(label,
                                                              bean[metric] if metric in bean and bean[metric] else 0)
