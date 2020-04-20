#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from prometheus_client.core import GaugeMetricFamily

import utils
from utils import get_module_logger

logger = get_module_logger(__name__)


class MetricCollector(object):
    def __init__(self, cluster, url, component, service):
        self._cluster = cluster
        self._url = url.rstrip('/')
        self._component = component
        self._prefix = 'hadoop_{0}_{1}'.format(component, service)

        self._file_list = utils.get_file_list(service)
        self._common_file = utils.get_file_list("common")
        self._merge_list = self._file_list + self._common_file

        self._metrics = {}
        for i in range(len(self._file_list)):
            self._metrics.setdefault(
                self._file_list[i], utils.read_json_file(service, self._file_list[i]))

    def collect(self):
        pass

    def _setup_metrics_labels(self):
        pass

    def _get_metrics(self, metrics):
        pass


def common_metrics_collector(cluster, beans, component, service, _target):
    tmp_metrics = {}
    common_metrics = {}
    _cluster = cluster
    _prefix = 'hadoop_{0}_{1}'.format(component, service)
    _metrics_type = utils.get_file_list("common")

    for i in range(len(_metrics_type)):
        common_metrics.setdefault(_metrics_type[i], {})
        tmp_metrics.setdefault(_metrics_type[i], utils.read_json_file("common", _metrics_type[i]))

    def setup_jvm_labels():
        for metric in tmp_metrics["JvmMetrics"]:
            '''
            Processing module JvmMetrics
            '''
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
                    descriptions = tmp_metrics['JvmMetrics'][metric]
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
                    descriptions = tmp_metrics['JvmMetrics'][metric]
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
                descriptions = tmp_metrics['JvmMetrics'][metric]
            label.append("_target")
            common_metrics['JvmMetrics'][key] = GaugeMetricFamily("_".join([_prefix, key]), descriptions, labels=label)
        return common_metrics

    def setup_os_labels():
        for metric in tmp_metrics['OperatingSystem']:
            label = ["cluster", "_target"]
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            common_metrics['OperatingSystem'][metric] = GaugeMetricFamily("_".join([_prefix, snake_case]), tmp_metrics['OperatingSystem'][metric], labels=label)
        return common_metrics

    def setup_rpc_labels():
        num_rpc_flag, avg_rpc_flag = 1, 1
        for metric in tmp_metrics["RpcActivity"]:
            '''
            Processing module RpcActivity, when multiple RpcActivity module exist,
            `tag.port` should be an identifier to distinguish each module.
            '''
            if 'Rpc' in metric:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            else:
                snake_case = "_".join(["rpc", re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()])
            label = ["cluster", "tag"]
            if "NumOps" in metric:
                if num_rpc_flag:
                    key = "MethodNumOps"
                    label.append("method")
                    label.append("_target")
                    name = "_".join([_prefix, "rpc_method_called_total"])
                    description = "Total number of the times the method is called."
                    common_metrics['RpcActivity'][key] = GaugeMetricFamily(name, description, labels=label)
                    num_rpc_flag = 0
                else:
                    continue
            elif "AvgTime" in metric:
                if avg_rpc_flag:
                    key = "MethodAvgTime"
                    label.append("method")
                    label.append("_target")
                    name = "_".join([_prefix, "rpc_method_avg_time_milliseconds"])
                    descrption = "Average turn around time of the method in milliseconds."
                    common_metrics['RpcActivity'][key] = GaugeMetricFamily(name, descrption, labels=label)
                    avg_rpc_flag = 0
                else:
                    continue
            else:
                key = metric
                label.append("_target")
                common_metrics['RpcActivity'][key] = GaugeMetricFamily("_".join([_prefix, snake_case]), tmp_metrics['RpcActivity'][metric], labels=label)
        return common_metrics

    def setup_rpc_detailed_labels():
        for metric in tmp_metrics['RpcDetailedActivity']:
            label = ["cluster", "tag"]
            if "NumOps" in metric:
                key = "NumOps"
                label.append("method")
                name = "_".join([_prefix, 'rpc_detailed_method_called_total'])
            elif "AvgTime" in metric:
                key = "AvgTime"
                label.append("method")
                name = "_".join([_prefix, 'rpc_detailed_method_avg_time_milliseconds'])
            else:
                pass
            label.append("_target")
            common_metrics['RpcDetailedActivity'][key] = GaugeMetricFamily(name, tmp_metrics['RpcDetailedActivity'][metric], labels=label)
        return common_metrics

    def setup_ugi_labels():
        ugi_num_flag, ugi_avg_flag = 1, 1
        for metric in tmp_metrics['UgiMetrics']:
            label = ["cluster"]
            if 'NumOps' in metric:
                if ugi_num_flag:
                    key = 'NumOps'
                    label.extend(["method", "state"])
                    ugi_num_flag = 0
                    label.append("_target")
                    name = "_".join([_prefix, 'ugi_method_called_total'])
                    description = "Total number of the times the method is called."
                    common_metrics['UgiMetrics'][key] = GaugeMetricFamily(name, description, labels=label)
                else:
                    continue
            elif 'AvgTime' in metric:
                if ugi_avg_flag:
                    key = 'AvgTime'
                    label.extend(["method", "state"])
                    ugi_avg_flag = 0
                    label.append("_target")
                    name = "_".join([_prefix, 'ugi_method_avg_time_milliseconds'])
                    description = "Average turn around time of the method in milliseconds."
                    common_metrics['UgiMetrics'][key] = GaugeMetricFamily(name, description, labels=label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label.append("_target")
                common_metrics['UgiMetrics'][metric] = GaugeMetricFamily("_".join([_prefix, 'ugi', snake_case]), tmp_metrics['UgiMetrics'][metric], labels=label)
        return common_metrics

    def setup_metric_system_labels():
        metric_num_flag, metric_avg_flag = 1, 1
        for metric in tmp_metrics['MetricsSystem']:
            label = ["cluster"]
            if 'NumOps' in metric:
                if metric_num_flag:
                    key = 'NumOps'
                    label.append("oper")
                    label.append("_target")
                    metric_num_flag = 0
                    common_metrics['MetricsSystem'][key] = GaugeMetricFamily("_".join([_prefix, 'metricssystem_operations_total']), "Total number of operations", labels=label)
                else:
                    continue
            elif 'AvgTime' in metric:
                if metric_avg_flag:
                    key = 'AvgTime'
                    label.append("oper")
                    label.append("_target")
                    metric_avg_flag = 0
                    name = "_".join([_prefix, 'metricssystem_method_avg_time_milliseconds'])
                    description = "Average turn around time of the operations in milliseconds."
                    common_metrics['MetricsSystem'][key] = GaugeMetricFamily(name, description, labels=label)
                else:
                    continue
            else:
                label.append("_target")
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([_prefix, 'metricssystem', snake_case])
                common_metrics['MetricsSystem'][metric] = GaugeMetricFamily(name, tmp_metrics['MetricsSystem'][metric], labels=label)
        return common_metrics

    def setup_runtime_labels():
        for metric in tmp_metrics['Runtime']:
            label = ["cluster", "host"]
            label.append("_target")
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            common_metrics['Runtime'][metric] = GaugeMetricFamily("_".join([_prefix, snake_case, "milliseconds"]), tmp_metrics['Runtime'][metric], labels=label)
        return common_metrics

    def setup_labels(beans):
        '''
        预处理，分析各个模块的特点，进行分类，添加label
        '''
        for i in range(len(beans)):
            if 'name=JvmMetrics' in beans[i]['name']:
                setup_jvm_labels()
            if 'OperatingSystem' in beans[i]['name']:
                setup_os_labels()
            if 'RpcActivity' in beans[i]['name']:
                setup_rpc_labels()
            if 'RpcDetailedActivity' in beans[i]['name']:
                setup_rpc_detailed_labels()
            if 'UgiMetrics' in beans[i]['name']:
                setup_ugi_labels()
            if 'MetricsSystem' in beans[i]['name'] and "sub=Stats" in beans[i]['name']:
                setup_metric_system_labels()
            if 'Runtime' in beans[i]['name']:
                setup_runtime_labels()
        return common_metrics

    def get_jvm_metrics(bean):
        for metric in tmp_metrics['JvmMetrics']:
            name = "_".join(
                ["jvm", re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()])
            if 'Mem' in metric:
                if "Used" in metric:
                    key = "jvm_mem_used_mebibytes"
                    mode = metric.split("Used")[0].split("Mem")[1]
                    label = [_cluster, mode]
                elif "Committed" in metric:
                    key = "jvm_mem_committed_mebibytes"
                    mode = metric.split("Committed")[0].split("Mem")[1]
                    label = [_cluster, mode]
                elif "Max" in metric:
                    key = "jvm_mem_max_mebibytes"
                    if "Heap" in metric:
                        mode = metric.split("Max")[0].split("Mem")[1]
                    else:
                        mode = "max"
                    label = [_cluster, mode]
                else:
                    key = "".join([name, 'ebibytes'])
                    label = [_cluster]
            elif 'Gc' in metric:
                if "GcCount" in metric:
                    key = "jvm_gc_count"
                    if "GcCount" == metric:
                        typo = "total"
                    else:
                        typo = metric.split("GcCount")[1]
                    label = [_cluster, typo]
                elif "GcTimeMillis" in metric:
                    key = "jvm_gc_time_milliseconds"
                    if "GcTimeMillis" == metric:
                        typo = "total"
                    else:
                        typo = metric.split("GcTimeMillis")[1]
                    label = [_cluster, typo]
                elif "ThresholdExceeded" in metric:
                    key = "jvm_gc_exceeded_threshold_total"
                    typo = metric.split("ThresholdExceeded")[
                        0].split("GcNum")[1]
                    label = [_cluster, typo]
                else:
                    key = name
                    label = [_cluster]
            elif 'Threads' in metric:
                key = "jvm_threads_state_total"
                state = metric.split("Threads")[1]
                label = [_cluster, state]
            elif 'Log' in metric:
                key = "jvm_log_level_total"
                level = metric.split("Log")[1]
                label = [_cluster, level]
            else:
                key = name
                label = [_cluster]
            label.append(_target)
            common_metrics['JvmMetrics'][key].add_metric(label, bean[metric] if metric in bean else 0)
        return common_metrics

    def get_os_metrics(bean):
        for metric in tmp_metrics['OperatingSystem']:
            label = [_cluster]
            label.append(_target)
            common_metrics['OperatingSystem'][metric].add_metric(label, bean[metric] if metric in bean else 0)
        return common_metrics

    def get_rpc_metrics(bean):
        rpc_tag = bean['tag.port']
        for metric in tmp_metrics['RpcActivity']:
            if "NumOps" in metric:
                method = metric.split('NumOps')[0]
                label = [_cluster, rpc_tag, method]
                key = "MethodNumOps"
            elif "AvgTime" in metric:
                method = metric.split('AvgTime')[0]
                label = [_cluster, rpc_tag, method]
                key = "MethodAvgTime"
            else:
                label = [_cluster, rpc_tag]
                key = metric
            label.append(_target)
            common_metrics['RpcActivity'][key].add_metric(label, bean[metric] if metric in bean else 0)
        return common_metrics

    def get_rpc_detailed_metrics(bean):
        detail_tag = bean['tag.port']
        for metric in bean:
            if metric[0].isupper():
                label = [_cluster, detail_tag]
                if "NumOps" in metric:
                    key = "NumOps"
                    method = metric.split('NumOps')[0]
                    label = [_cluster, detail_tag, method]
                elif "AvgTime" in metric:
                    key = "AvgTime"
                    method = metric.split("AvgTime")[0]
                    label = [_cluster, detail_tag, method]
                else:
                    pass
                label.append(_target)
                common_metrics['RpcDetailedActivity'][key].add_metric(label, bean[metric])
        return common_metrics

    def get_ugi_metrics(bean):
        for metric in tmp_metrics['UgiMetrics']:
            if 'NumOps' in metric:
                key = 'NumOps'
                if 'Login' in metric:
                    method = 'Login'
                    state = metric.split('Login')[1].split('NumOps')[0]
                    label = [_cluster, method, state]
                else:
                    method = metric.split('NumOps')[0]
                    label = [_cluster, method, "-"]
                # common_metrics['UgiMetrics'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
            elif 'AvgTime' in metric:
                key = 'AvgTime'
                if 'Login' in metric:
                    method = 'Login'
                    state = metric.split('Login')[1].split('AvgTime')[0]
                    label = [_cluster, method, state]
                else:
                    method = metric.split('AvgTime')[0]
                    label = [_cluster, method, "-"]
                # common_metrics['UgiMetrics'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
            else:
                key = metric
                label = [_cluster, "-", "-"]
            label.append(_target)
            common_metrics['UgiMetrics'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
        return common_metrics

    def get_metric_system_metrics(bean):
        for metric in tmp_metrics['MetricsSystem']:
            if 'NumOps' in metric:
                key = 'NumOps'
                oper = metric.split('NumOps')[0]
                label = [_cluster, oper]
            elif 'AvgTime' in metric:
                key = 'AvgTime'
                oper = metric.split('AvgTime')[0]
                label = [_cluster, oper]
            else:
                key = metric
                label = [_cluster]
            label.append(_target)
            common_metrics['MetricsSystem'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
        return common_metrics

    def get_runtime_metrics(bean):
        for metric in tmp_metrics['Runtime']:
            label = [_cluster, bean['Name'].split("@")[1], "_target"]
            common_metrics['Runtime'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
        return common_metrics

    def get_metrics():
        common_metrics = setup_labels(beans)
        for i in range(len(beans)):
            if 'name=JvmMetrics' in beans[i]['name']:
                get_jvm_metrics(beans[i])
            if 'OperatingSystem' in beans[i]['name']:
                get_os_metrics(beans[i])
            if 'RpcActivity' in beans[i]['name']:
                get_rpc_metrics(beans[i])
            if 'RpcDetailedActivity' in beans[i]['name']:
                get_rpc_detailed_metrics(beans[i])
            if 'UgiMetrics' in beans[i]['name']:
                get_ugi_metrics(beans[i])
            if 'MetricsSystem' in beans[i]['name'] and "sub=Stats" in beans[i]['name']:
                get_metric_system_metrics(beans[i])
            if 'Runtime' in beans[i]['name']:
                get_runtime_metrics(beans[i])
        return common_metrics

    return get_metrics
