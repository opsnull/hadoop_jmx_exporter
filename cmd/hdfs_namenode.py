#!/usr/bin/python
# -*- coding: utf-8 -*-

import yaml
import re

from prometheus_client.core import GaugeMetricFamily

import utils
from utils import get_module_logger
from common import MetricCollector, common_metrics_collector

logger = get_module_logger(__name__)


class NameNodeMetricCollector(MetricCollector):

    def __init__(self, cluster, url):
        MetricCollector.__init__(self, cluster, url, "hdfs", "namenode")
        self._target = "-"
        self._hadoop_namenode_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_namenode_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        try:
            beans = utils.get_metrics(self._url)
        except:
            logger.info("Can't scrape metrics from url: {0}".format(self._url))
        else:
            for i in range(len(beans)):
                if 'name=FSNamesystem' in beans[i]['name']:
                    self._target = beans[i]["tag.Hostname"]
                    break
            # set up all metrics with labels and descriptions.
            self._setup_metrics_labels(beans)

            # add metric value to every metric.
            self._get_metrics(beans)

            # update namenode metrics with common metrics
            common_metrics = common_metrics_collector(self._cluster, beans, "hdfs", "namenode", self._target)
            self._hadoop_namenode_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_namenode_metrics[service]:
                    yield self._hadoop_namenode_metrics[service][metric]

    def _setup_nnactivity_labels(self):
        num_namenode_flag, avg_namenode_flag, ops_namenode_flag = 1, 1, 1
        for metric in self._metrics['NameNodeActivity']:
            label = ["cluster", "method", "_target"]
            if "NumOps" in metric:
                if num_namenode_flag:
                    key = "MethodNumOps"
                    name = "_".join([self._prefix, "nnactivity_method_ops_total"])
                    description = "Total number of the times the method is called."
                    self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(name, description, labels=label)
                    num_namenode_flag = 0
                else:
                    continue
            elif "AvgTime" in metric:
                if avg_namenode_flag:
                    key = "MethodAvgTime"
                    name = "_".join([self._prefix, "nnactivity_method_avg_time_milliseconds"])
                    descripton = "Average turn around time of the method in milliseconds."
                    self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(name, descripton, labels=label)
                    avg_namenode_flag = 0
                else:
                    continue
            else:
                if ops_namenode_flag:
                    ops_namenode_flag = 0
                    key = "Operations"
                    name = "_".join([self._prefix, "nnactivity_operations_total"])
                    description = "Total number of each operation."
                    self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(name, description, labels=label)
                else:
                    continue

    def _setup_startupprogress_labels(self):
        sp_count_flag, sp_elapsed_flag, sp_total_flag, sp_complete_flag = 1, 1, 1, 1
        for metric in self._metrics['StartupProgress']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if "ElapsedTime" == metric:
                key = "ElapsedTime"
                name = "total_elapsed_time_milliseconds"
                descriptions = "Total elapsed time in milliseconds."
            elif "PercentComplete" == metric:
                key = "PercentComplete"
                name = "complete_rate"
                descriptions = "Current rate completed in NameNode startup progress  (The max value is not 100 but 1.0)."
            elif "Count" in metric:
                if sp_count_flag:
                    sp_count_flag = 0
                    key = "PhaseCount"
                    name = "phase_count"
                    descriptions = "Total number of steps completed in the phase."
                else:
                    continue
            elif "ElapsedTime" in metric:
                if sp_elapsed_flag:
                    sp_elapsed_flag = 0
                    key = "PhaseElapsedTime"
                    name = "phase_elapsed_time_milliseconds"
                    descriptions = "Total elapsed time in the phase in milliseconds."
                else:
                    continue
            elif "Total" in metric:
                if sp_total_flag:
                    sp_total_flag = 0
                    key = "PhaseTotal"
                    name = "phase_total"
                    descriptions = "Total number of steps in the phase."
                else:
                    continue
            elif "PercentComplete" in metric:
                if sp_complete_flag:
                    sp_complete_flag = 0
                    key = "PhasePercentComplete"
                    name = "phase_complete_rate"
                    descriptions = "Current rate completed in the phase  (The max value is not 100 but 1.0)."
                else:
                    continue
            else:
                key = metric
                name = snake_case
                descriptions = self._metrics['StartupProgress'][metric]
            label = ["cluster", "phase", "_target"]
            name = "_".join([self._prefix, "startup_process", name])
            self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(name, descriptions, labels=label)

    def _setup_fsnamesystem_labels(self):
        cap_flag = 1
        for metric in self._metrics['FSNamesystem']:
            if metric.startswith('Capacity'):
                if cap_flag:
                    cap_flag = 0
                    key = "capacity"
                    label = ["cluster", "mode"]
                    name = "capacity_bytes"
                    descriptions = "Current DataNodes capacity in each mode in bytes"
                else:
                    continue
            else:
                key = metric
                label = ["cluster"]
                name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                descriptions = self._metrics['FSNamesystem'][metric]
            label.append("_target")
            name = "_".join([self._prefix, "fsname_system", name])
            self._hadoop_namenode_metrics['FSNamesystem'][key] = GaugeMetricFamily(name, descriptions, labels=label)

    def _setup_fsnamesystem_state_labels(self):
        num_flag = 1
        for metric in self._metrics['FSNamesystemState']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if 'DataNodes' in metric:
                if num_flag:
                    num_flag = 0
                    key = "datanodes_num"
                    label = ["cluster", "state"]
                    descriptions = "Number of datanodes in each state"
                else:
                    continue
            else:
                key = metric
                label = ["cluster"]
                descriptions = self._metrics['FSNamesystemState'][metric]
            label.append("_target")
            name = "_".join([self._prefix, "fsname_system_state", snake_case])
            self._hadoop_namenode_metrics['FSNamesystemState'][key] = GaugeMetricFamily(name, descriptions, labels=label)

    def _setup_retrycache_labels(self):
        cache_flag = 1
        for metric in self._metrics['RetryCache']:
            if cache_flag:
                cache_flag = 0
                key = "cache"
                label = ["cluster", "mode", "_target"]
                name = "_".join([self._prefix, "cache_total"])
                description = "Total number of RetryCache in each mode"
                self._hadoop_namenode_metrics['RetryCache'][key] = GaugeMetricFamily(name, description, labels=label)

    def _setup_nninfo_labels(self):
        for metric in self._metrics['NameNodeInfo']:
            if "LiveNodes" in metric:
                name = "_".join([self._prefix, "nninfo_live_nodes_count"])
                description = "Count of live data node"
                self._hadoop_namenode_metrics['NameNodeInfo']["LiveNodeCount"] = GaugeMetricFamily(name, description, labels=["cluster", "_target"])

                label = ["cluster", "datanode", "infoAddr", "infoSecureAddr", "xferaddr", "version", "_target"]
                items = ["lastContact", "usedSpace", "adminState", "nonDfsUsedSpace", "capacity", "numBlocks",
                         "used", "remaining", "blockScheduled", "blockPoolUsed", "blockPoolUsedPercent", "volfails"]
                for item in items:
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    name = "_".join([self._prefix, "nninfo_live_nodes", item])
                    key = "LiveNodes-" + item
                    description = "Live node " + item
                    if item == "admin_state":
                        description += " 0: In Service, 1: Decommission In Progress, 2: Decommissioned"
                    self._hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "DeadNodes" in metric:
                name = "_".join([self._prefix, "nninfo_dead_nodes_count"])
                description = "Count of dead data node"
                self._hadoop_namenode_metrics['NameNodeInfo']["DeadNodeCount"] = GaugeMetricFamily(name, description, labels=["cluster", "_target"])

                label = ["cluster", "datanode", "decommissioned", "xferaddr"]
                name = "_".join([self._prefix, "nninfo_dead_nodes_last_contact"])
                key = "DeadNodes"
                description = "Dead node last contact in milions"
                self._hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "DecomNodes" in metric:
                name = "_".join([self._prefix, "nninfo_decom_nodes_count"])
                description = "Count of decommissioned data node"
                self._hadoop_namenode_metrics['NameNodeInfo']["DecomNodeCount"] = GaugeMetricFamily(name, description, labels=["cluster", "_target"])

                label = ["cluster", "datanode", "xferaddr", "_target"]
                items = ["underReplicatedBlocks", "decommissionOnlyReplicas", "underReplicateInOpenFiles"]
                for item in items:
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    name = "_".join([self._prefix, "nninfo_decom_nodes", item])
                    key = "DecomNodes-" + item
                    description = "Decom Node " + item
                    self._hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "EnteringMaintenanceNodes" in metric:
                name = "_".join([self._prefix, "nninfo_maintenance_nodes_count"])
                description = "Count of maintenance data node"
                self._hadoop_namenode_metrics['NameNodeInfo']["MaintenanceNodeCount"] = GaugeMetricFamily(name, description, labels=["cluster", "_target"])

                label = ["cluster", "datanode", "xferaddr", "_target"]
                items = ["underReplicatedBlocks", "maintenanceOnlyReplicas", "underReplicateInOpenFiles"]
                for item in items:
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    name = "_".join([self._prefix, "nninfo_entering_maintenance_nodes", item])
                    key = "EnteringMaintenanceNodes-" + item
                    description = "Entering maintenance node " + item
                    self._hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "CorruptFiles" in metric:
                label = ["cluster", "_target"]
                name = "_".join([self._prefix, "nninfo_corrupt_file_count"])
                key = "CorruptFiles"
                description = "Corrupt file count"
                self._hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "NodeUsage" in metric:
                label = ["cluster", "_target"]
                items = ["min", "median", "max", "stdDev"]
                for item in items:
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    name = "_".join([self._prefix, "nninfo_node_usage", item])
                    key = "NodeUsage-" + item
                    description = "Node usage " + item
                    self._hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "SoftwareVersion" in metric:
                label = ["cluster", "software_version"]
                name = "_".join([self._prefix, "nninfo_software_version"])
                key = "SoftwareVersion"
            elif "Safemode" in metric:
                label = ["cluster"]
                name = "_".join([self._prefix, "nninfo_safe_mode"])
                key = "Safemode"
            else:
                label = ["cluster"]
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self._prefix, "nninfo", snake_case])
                key = metric
            label.append("_target")
            self._hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, self._metrics["NameNodeInfo"][metric], labels=label)

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for i in range(len(beans)):
            if 'NameNodeActivity' in beans[i]['name']:
                self._setup_nnactivity_labels()
            if 'StartupProgress' in beans[i]['name']:
                self._setup_startupprogress_labels()
            if 'FSNamesystem' in beans[i]['name']:
                self._setup_fsnamesystem_labels()
            if 'FSNamesystemState' in beans[i]['name']:
                self._setup_fsnamesystem_state_labels()
            if 'RetryCache' in beans[i]['name']:
                self._setup_retrycache_labels()
            if "NameNodeInfo" in beans[i]['name']:
                self._setup_nninfo_labels()

    def _get_nnactivity_metrics(self, bean):
        for metric in self._metrics['NameNodeActivity']:
            if "NumOps" in metric:
                method = metric.split('NumOps')[0]
                key = "MethodNumOps"
            elif "AvgTime" in metric:
                method = metric.split('AvgTime')[0]
                key = "MethodAvgTime"
            else:
                if "Ops" in metric:
                    method = metric.split('Ops')[0]
                else:
                    method = metric
                key = "Operations"
            label = [self._cluster, method, self._target]
            self._hadoop_namenode_metrics['NameNodeActivity'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def _get_startupprogress_metrics(self, bean):
        for metric in self._metrics['StartupProgress']:
            if "Count" in metric:
                key = "PhaseCount"
                phase = metric.split("Count")[0]
            elif "ElapsedTime" in metric and "ElapsedTime" != metric:
                key = "PhaseElapsedTime"
                phase = metric.split("ElapsedTime")[0]
            elif "Total" in metric:
                key = "PhaseTotal"
                phase = metric.split("Total")[0]
            elif "PercentComplete" in metric and "PercentComplete" != metric:
                key = "PhasePercentComplete"
                phase = metric.split("PercentComplete")[0]
            else:
                key = metric
                phase = "-"
            label = [self._cluster, phase, self._target]
            self._hadoop_namenode_metrics['StartupProgress'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def _get_fsnamesystem_metrics(self, bean):
        for metric in self._metrics['FSNamesystem']:
            key = metric
            if 'HAState' in metric:
                label = [self._cluster]
                if 'initializing' == bean['tag.HAState']:
                    value = 0.0
                elif 'active' == bean['tag.HAState']:
                    value = 1.0
                elif 'standby' == bean['tag.HAState']:
                    value = 2.0
                elif 'stopping' == bean['tag.HAState']:
                    value = 3.0
                else:
                    value = 9999
                label.append(self._target)
                self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, value)
            elif metric.startswith("Capacity"):
                key = 'capacity'
                mode = metric.split("Capacity")[1]
                label = [self._cluster, mode]
                label.append(self._target)
                self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, bean[metric] if metric in bean else 0)
            else:
                label = [self._cluster]
                label.append(self._target)
                self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def _get_fsnamesystem_state_metrics(self, bean):
        for metric in self._metrics['FSNamesystemState']:
            label = [self._cluster]
            key = metric
            if 'FSState' in metric:
                if 'Safemode' == bean['FSState']:
                    value = 0.0
                elif 'Operational' == bean['FSState']:
                    value = 1.0
                else:
                    value = 9999
                label.append(self._target)
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, value)
            elif "TotalSyncTimes" in metric:
                label.append(self._target)
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, float(
                    re.sub(r'\s', '', bean[metric])) if metric in bean and bean[metric] else 0)
            elif "DataNodes" in metric:
                key = 'datanodes_num'
                state = metric.split("DataNodes")[0].split("Num")[1]
                label = [self._cluster, state]
                label.append(self._target)
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
            else:
                label.append(self._target)
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

    def _get_retrycache_metrics(self, bean):
        for metric in self._metrics['RetryCache']:
            key = "cache"
            label = [self._cluster, metric.split('Cache')[1]]
            label.append(self._target)
            self._hadoop_namenode_metrics['RetryCache'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

    def _get_nninfo_metrics(self, bean):
        for metric in self._metrics["NameNodeInfo"]:
            if "LiveNodes" in metric and "LiveNodes" in bean:
                live_node_dict = yaml.safe_load(bean["LiveNodes"])
                self._hadoop_namenode_metrics["NameNodeInfo"]["LiveNodeCount"].add_metric([self._cluster, self._target], len(live_node_dict))
                for node, info in live_node_dict.items():
                    label = [self._cluster, node, info["infoAddr"], info["infoSecureAddr"], info["xferaddr"], info["version"], self._target]
                    items = ["lastContact", "usedSpace", "adminState", "nonDfsUsedSpace", "capacity", "numBlocks",
                             "used", "remaining", "blockScheduled", "blockPoolUsed", "blockPoolUsedPercent", "volfails"]
                    for item in items:
                        value = info[item] if item in info else 0
                        if item == "adminState":
                            if value == "In Service":
                                value = 0
                            elif value == "Decommission In Progress":
                                value = 1
                            else:  # Decommissioned
                                value = 2
                        item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                        key = "LiveNodes-" + item
                        self._hadoop_namenode_metrics["NameNodeInfo"][key].add_metric(label, value)
            elif "DeadNodes" in metric and "DeadNodes" in bean:
                dead_node_dict = yaml.safe_load(bean["DeadNodes"])
                self._hadoop_namenode_metrics["NameNodeInfo"]["DeadNodeCount"].add_metric([self._cluster, self._target], len(dead_node_dict))
                for node, info in dead_node_dict.items():
                    label = [self._cluster, node, str(info["decommissioned"]), info["xferaddr"], self._target]
                    value = info["lastContact"]
                    self._hadoop_namenode_metrics["NameNodeInfo"]["DeadNodes"].add_metric(label, value)
            elif "DecomNodes" in metric and "DecomNodes" in bean:
                decom_node_dict = yaml.safe_load(bean["DecomNodes"])
                self._hadoop_namenode_metrics["NameNodeInfo"]["DecomNodeCount"].add_metric([self._cluster, self._target], len(decom_node_dict))
                for node, info in decom_node_dict.items():
                    label = [self._cluster, node, info["xferaddr"], self._target]
                    items = ["underReplicatedBlocks", "decommissionOnlyReplicas", "underReplicateInOpenFiles"]
                    for item in items:
                        value = info[item] if item in info else 0
                        item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                        key = "DecomNodes-" + item
                        self._hadoop_namenode_metrics["NameNodeInfo"][key].add_metric(label, value)
            elif "EnteringMaintenanceNodes" in metric and "EnteringMaintenanceNodes" in bean:
                node_dict = yaml.safe_load(bean["EnteringMaintenanceNodes"])
                self._hadoop_namenode_metrics["NameNodeInfo"]["MaintenanceNodeCount"].add_metric([self._cluster, self._target], len(node_dict))
                for node, info in node_dict.items():
                    label = [self._cluster, node, info["xferaddr"], self._target]
                    items = ["underReplicatedBlocks", "maintenanceOnlyReplicas", "underReplicateInOpenFiles"]
                    for item in items:
                        value = info[item] if item in info else 0
                        item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                        key = "EnteringMaintenanceNodes-" + item
                        self._hadoop_namenode_metrics["NameNodeInfo"][key].add_metric(label, value)
            elif "CorruptFiles" in metric and "CorruptFiles" in bean:
                file_list = yaml.safe_load(bean["CorruptFiles"])
                label = [self._cluster, self._target]
                self._hadoop_namenode_metrics["NameNodeInfo"]["CorruptFiles"].add_metric(label, len(file_list))
            elif "NodeUsage" in metric and "NodeUsage" in bean:
                node_usage_dict = yaml.safe_load(bean["NodeUsage"])["nodeUsage"]
                label = [self._cluster, self._target]
                items = ["min", "median", "max", "stdDev"]
                for item in items:
                    value = node_usage_dict[item] if item in node_usage_dict else 0
                    value = float(value.strip("%"))
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    key = "NodeUsage-" + item
                    self._hadoop_namenode_metrics["NameNodeInfo"][key].add_metric(label, value)
            elif "SoftwareVersion" in metric and "SoftwareVersion" in bean:
                label = [self._cluster, bean["SoftwareVersion"], self._target]
                self._hadoop_namenode_metrics["NameNodeInfo"]["SoftwareVersion"].add_metric(label, 0)
            elif "Safemode" in metric and "Safemode" in bean:
                label = [self._cluster, self._target]
                self._hadoop_namenode_metrics["NameNodeInfo"]["Safemode"].add_metric(label, 0 if metric in bean and bean[metric] == "" else 1)
            else:
                label = [self._cluster, self._target]
                self._hadoop_namenode_metrics['NameNodeInfo'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

    def _get_metrics(self, beans):
        for i in range(len(beans)):
            if 'NameNodeActivity' in beans[i]['name']:
                self._get_nnactivity_metrics(beans[i])
            if 'StartupProgress' in beans[i]['name']:
                self._get_startupprogress_metrics(beans[i])
            if 'FSNamesystem' in beans[i]['name'] and 'FSNamesystemState' not in beans[i]['name']:
                self._get_fsnamesystem_metrics(beans[i])
            if 'FSNamesystemState' in beans[i]['name']:
                self._get_fsnamesystem_state_metrics(beans[i])
            if 'RetryCache' in beans[i]['name']:
                self._get_retrycache_metrics(beans[i])
            if 'NameNodeInfo' in beans[i]['name']:
                self._get_nninfo_metrics(beans[i])
