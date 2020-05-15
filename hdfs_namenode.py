#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import yaml
import re

from prometheus_client.core import GaugeMetricFamily

from utils import get_module_logger
from common import MetricCollector, CommonMetricCollector
from scraper import ScrapeMetrics

logger = get_module_logger(__name__)


class NameNodeMetricCollector(MetricCollector):
    def __init__(self, cluster, urls):
        MetricCollector.__init__(self, cluster, "hdfs", "namenode")
        self.target = "-"
        self.urls = urls
        self.dns = set()

        self.hadoop_namenode_metrics = {}
        for i in range(len(self.file_list)):
            self.hadoop_namenode_metrics.setdefault(self.file_list[i], {})

        self.common_metric_collector = CommonMetricCollector(cluster, "hdfs", "namenode")

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
            self.hadoop_namenode_metrics.update(self.common_metric_collector.get_metrics(beans, self.target))
            self.get_metrics(beans)

        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            if service in self.hadoop_namenode_metrics:
                for metric in self.hadoop_namenode_metrics[service]:
                    yield self.hadoop_namenode_metrics[service][metric]

    def setup_nnactivity_labels(self):
        num_namenode_flag, avg_namenode_flag, ops_namenode_flag = 1, 1, 1
        for metric in self.metrics['NameNodeActivity']:
            label = ["cluster", "method", "_target"]
            if "NumOps" in metric:
                if num_namenode_flag:
                    key = "MethodNumOps"
                    name = "_".join([self.prefix, "nnactivity_method_ops_total"])
                    description = "Total number of the times the method is called."
                    self.hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(name, description, labels=label)
                    num_namenode_flag = 0
                else:
                    continue
            elif "AvgTime" in metric:
                if avg_namenode_flag:
                    key = "MethodAvgTime"
                    name = "_".join([self.prefix, "nnactivity_method_avg_time_milliseconds"])
                    descripton = "Average turn around time of the method in milliseconds."
                    self.hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(name, descripton, labels=label)
                    avg_namenode_flag = 0
                else:
                    continue
            elif ops_namenode_flag:
                key = "Operations"
                name = "_".join([self.prefix, "nnactivity_operations_total"])
                description = "Total number of each operation."
                self.hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(name, description, labels=label)
                ops_namenode_flag = 0

    def setup_startupprogress_labels(self):
        sp_count_flag, sp_elapsed_flag, sp_total_flag, sp_complete_flag = 1, 1, 1, 1
        for metric in self.metrics['StartupProgress']:
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
                descriptions = self.metrics['StartupProgress'][metric]
            label = ["cluster", "phase", "_target"]
            name = "_".join([self.prefix, "startup_process", name])
            self.hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(name, descriptions, labels=label)

    def setup_fsnamesystem_labels(self):
        cap_flag = 1
        for metric in self.metrics['FSNamesystem']:
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
                descriptions = self.metrics['FSNamesystem'][metric]
            label.append("_target")
            name = "_".join([self.prefix, "fsname_system", name])
            self.hadoop_namenode_metrics['FSNamesystem'][key] = GaugeMetricFamily(name, descriptions, labels=label)

    def setup_fsnamesystem_state_labels(self):
        num_flag = 1
        for metric in self.metrics['FSNamesystemState']:
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
                descriptions = self.metrics['FSNamesystemState'][metric]
            label.append("_target")
            name = "_".join([self.prefix, "fsname_system_state", snake_case])
            self.hadoop_namenode_metrics['FSNamesystemState'][key] = GaugeMetricFamily(name, descriptions, labels=label)

    def setup_retrycache_labels(self):
        cache_flag = 1
        for metric in self.metrics['RetryCache']:
            if cache_flag:
                cache_flag = 0
                key = "cache"
                label = ["cluster", "mode", "_target"]
                name = "_".join([self.prefix, "cache_total"])
                description = "Total number of RetryCache in each mode"
                self.hadoop_namenode_metrics['RetryCache'][key] = GaugeMetricFamily(name, description, labels=label)

    def setup_nninfo_labels(self):
        for metric in self.metrics['NameNodeInfo']:
            if "LiveNodes" in metric:
                name = "_".join([self.prefix, "nninfo_live_nodes_count"])
                description = "Count of live data node"
                self.hadoop_namenode_metrics['NameNodeInfo']["LiveNodeCount"] = GaugeMetricFamily(name, description, labels=["cluster", "_target"])

                label = ["cluster", "datanode", "infoAddr", "infoSecureAddr", "xferaddr", "version", "_target"]
                items = ["lastContact", "usedSpace", "adminState", "nonDfsUsedSpace", "capacity", "numBlocks",
                         "used", "remaining", "blockScheduled", "blockPoolUsed", "blockPoolUsedPercent", "volfails"]
                for item in items:
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    name = "_".join([self.prefix, "nninfo_live_nodes", item])
                    key = "LiveNodes-" + item
                    description = "Live node " + item
                    if item == "admin_state":
                        description += " 0: In Service, 1: Decommission In Progress, 2: Decommissioned"
                    self.hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "DeadNodes" in metric:
                name = "_".join([self.prefix, "nninfo_dead_nodes_count"])
                description = "Count of dead data node"
                self.hadoop_namenode_metrics['NameNodeInfo']["DeadNodeCount"] = GaugeMetricFamily(name, description, labels=["cluster", "_target"])

                label = ["cluster", "datanode", "decommissioned", "xferaddr"]
                name = "_".join([self.prefix, "nninfo_dead_nodes_last_contact"])
                key = "DeadNodes"
                description = "Dead node last contact in milions"
                self.hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "DecomNodes" in metric:
                name = "_".join([self.prefix, "nninfo_decom_nodes_count"])
                description = "Count of decommissioned data node"
                self.hadoop_namenode_metrics['NameNodeInfo']["DecomNodeCount"] = GaugeMetricFamily(name, description, labels=["cluster", "_target"])

                label = ["cluster", "datanode", "xferaddr", "_target"]
                items = ["underReplicatedBlocks", "decommissionOnlyReplicas", "underReplicateInOpenFiles"]
                for item in items:
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    name = "_".join([self.prefix, "nninfo_decom_nodes", item])
                    key = "DecomNodes-" + item
                    description = "Decom Node " + item
                    self.hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "EnteringMaintenanceNodes" in metric:
                name = "_".join([self.prefix, "nninfo_maintenance_nodes_count"])
                description = "Count of maintenance data node"
                self.hadoop_namenode_metrics['NameNodeInfo']["MaintenanceNodeCount"] = GaugeMetricFamily(name, description, labels=["cluster", "_target"])

                label = ["cluster", "datanode", "xferaddr", "_target"]
                items = ["underReplicatedBlocks", "maintenanceOnlyReplicas", "underReplicateInOpenFiles"]
                for item in items:
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    name = "_".join([self.prefix, "nninfo_entering_maintenance_nodes", item])
                    key = "EnteringMaintenanceNodes-" + item
                    description = "Entering maintenance node " + item
                    self.hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "CorruptFiles" in metric:
                label = ["cluster", "_target"]
                name = "_".join([self.prefix, "nninfo_corrupt_file_count"])
                key = "CorruptFiles"
                description = "Corrupt file count"
                self.hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "NodeUsage" in metric:
                label = ["cluster", "_target"]
                items = ["min", "median", "max", "stdDev"]
                for item in items:
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    name = "_".join([self.prefix, "nninfo_node_usage", item])
                    key = "NodeUsage-" + item
                    description = "Node usage " + item
                    self.hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, description, labels=label)
                continue
            elif "SoftwareVersion" in metric:
                label = ["cluster", "software_version"]
                name = "_".join([self.prefix, "nninfo_software_version"])
                key = "SoftwareVersion"
            elif "Safemode" in metric:
                label = ["cluster"]
                name = "_".join([self.prefix, "nninfo_safe_mode"])
                key = "Safemode"
            else:
                label = ["cluster"]
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self.prefix, "nninfo", snake_case])
                key = metric
            label.append("_target")
            self.hadoop_namenode_metrics['NameNodeInfo'][key] = GaugeMetricFamily(name, self.metrics["NameNodeInfo"][metric], labels=label)

    def setup_metrics_labels(self, beans):
        for i in range(len(beans)):
            if 'NameNodeActivity' in beans[i]['name']:
                self.setup_nnactivity_labels()
            if 'StartupProgress' in beans[i]['name']:
                self.setup_startupprogress_labels()
            if 'FSNamesystem' in beans[i]['name']:
                self.setup_fsnamesystem_labels()
            if 'FSNamesystemState' in beans[i]['name']:
                self.setup_fsnamesystem_state_labels()
            if 'RetryCache' in beans[i]['name']:
                self.setup_retrycache_labels()
            if "NameNodeInfo" in beans[i]['name']:
                self.setup_nninfo_labels()

    def get_nnactivity_metrics(self, bean):
        for metric in self.metrics['NameNodeActivity']:
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
            label = [self.cluster, method, self.target]
            self.hadoop_namenode_metrics['NameNodeActivity'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def get_startupprogress_metrics(self, bean):
        for metric in self.metrics['StartupProgress']:
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
            label = [self.cluster, phase, self.target]
            self.hadoop_namenode_metrics['StartupProgress'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def get_fsnamesystem_metrics(self, bean):
        for metric in self.metrics['FSNamesystem']:
            key = metric
            if 'HAState' in metric:
                label = [self.cluster]
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
                label.append(self.target)
                self.hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, value)
            elif metric.startswith("Capacity"):
                key = 'capacity'
                mode = metric.split("Capacity")[1]
                label = [self.cluster, mode]
                label.append(self.target)
                self.hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, bean[metric] if metric in bean else 0)
            else:
                label = [self.cluster]
                label.append(self.target)
                self.hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def get_fsnamesystem_state_metrics(self, bean):
        for metric in self.metrics['FSNamesystemState']:
            label = [self.cluster]
            key = metric
            if 'FSState' in metric:
                if 'Safemode' == bean['FSState']:
                    value = 0.0
                elif 'Operational' == bean['FSState']:
                    value = 1.0
                else:
                    value = 9999
                label.append(self.target)
                self.hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, value)
            elif "TotalSyncTimes" in metric:
                label.append(self.target)
                self.hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, float(
                    re.sub(r'\s', '', bean[metric])) if metric in bean and bean[metric] else 0)
            elif "DataNodes" in metric:
                key = 'datanodes_num'
                state = metric.split("DataNodes")[0].split("Num")[1]
                label = [self.cluster, state, self.target]
                self.hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
            else:
                label.append(self.target)
                self.hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

    def get_retrycache_metrics(self, bean):
        for metric in self.metrics['RetryCache']:
            key = "cache"
            label = [self.cluster, metric.split('Cache')[1], self.target]
            self.hadoop_namenode_metrics['RetryCache'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

    def get_nninfo_metrics(self, bean):
        for metric in self.metrics["NameNodeInfo"]:
            if "LiveNodes" in metric and "LiveNodes" in bean:
                live_node_dict = yaml.safe_load(bean["LiveNodes"])
                self.hadoop_namenode_metrics["NameNodeInfo"]["LiveNodeCount"].add_metric([self.cluster, self.target], len(live_node_dict))
                dns = set()
                for node, info in live_node_dict.items():
                    label = [self.cluster, node, info["infoAddr"], info["infoSecureAddr"], info["xferaddr"], info["version"], self.target]
                    items = ["lastContact", "usedSpace", "adminState", "nonDfsUsedSpace", "capacity", "numBlocks",
                             "used", "remaining", "blockScheduled", "blockPoolUsed", "blockPoolUsedPercent", "volfails"]
                    dns.add("http://"+info["infoAddr"]+"/jmx")
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
                        self.hadoop_namenode_metrics["NameNodeInfo"][key].add_metric(label, value)
                self.dns = dns
            elif "DeadNodes" in metric and "DeadNodes" in bean:
                dead_node_dict = yaml.safe_load(bean["DeadNodes"])
                self.hadoop_namenode_metrics["NameNodeInfo"]["DeadNodeCount"].add_metric([self.cluster, self.target], len(dead_node_dict))
                for node, info in dead_node_dict.items():
                    label = [self.cluster, node, str(info["decommissioned"]), info["xferaddr"], self.target]
                    value = info["lastContact"]
                    self.hadoop_namenode_metrics["NameNodeInfo"]["DeadNodes"].add_metric(label, value)
            elif "DecomNodes" in metric and "DecomNodes" in bean:
                decom_node_dict = yaml.safe_load(bean["DecomNodes"])
                self.hadoop_namenode_metrics["NameNodeInfo"]["DecomNodeCount"].add_metric([self.cluster, self.target], len(decom_node_dict))
                for node, info in decom_node_dict.items():
                    label = [self.cluster, node, info["xferaddr"], self.target]
                    items = ["underReplicatedBlocks", "decommissionOnlyReplicas", "underReplicateInOpenFiles"]
                    for item in items:
                        value = info[item] if item in info else 0
                        item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                        key = "DecomNodes-" + item
                        self.hadoop_namenode_metrics["NameNodeInfo"][key].add_metric(label, value)
            elif "EnteringMaintenanceNodes" in metric and "EnteringMaintenanceNodes" in bean:
                node_dict = yaml.safe_load(bean["EnteringMaintenanceNodes"])
                self.hadoop_namenode_metrics["NameNodeInfo"]["MaintenanceNodeCount"].add_metric([self.cluster, self.target], len(node_dict))
                for node, info in node_dict.items():
                    label = [self.cluster, node, info["xferaddr"], self.target]
                    items = ["underReplicatedBlocks", "maintenanceOnlyReplicas", "underReplicateInOpenFiles"]
                    for item in items:
                        value = info[item] if item in info else 0
                        item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                        key = "EnteringMaintenanceNodes-" + item
                        self.hadoop_namenode_metrics["NameNodeInfo"][key].add_metric(label, value)
            elif "CorruptFiles" in metric and "CorruptFiles" in bean:
                file_list = yaml.safe_load(bean["CorruptFiles"])
                label = [self.cluster, self.target]
                self.hadoop_namenode_metrics["NameNodeInfo"]["CorruptFiles"].add_metric(label, len(file_list))
            elif "NodeUsage" in metric and "NodeUsage" in bean:
                node_usage_dict = yaml.safe_load(bean["NodeUsage"])["nodeUsage"]
                label = [self.cluster, self.target]
                items = ["min", "median", "max", "stdDev"]
                for item in items:
                    value = node_usage_dict[item] if item in node_usage_dict else 0
                    value = float(value.strip("%"))
                    item = re.sub('([a-z0-9])([A-Z])', r'\1_\2', item).lower()
                    key = "NodeUsage-" + item
                    self.hadoop_namenode_metrics["NameNodeInfo"][key].add_metric(label, value)
            elif "SoftwareVersion" in metric and "SoftwareVersion" in bean:
                label = [self.cluster, bean["SoftwareVersion"], self.target]
                self.hadoop_namenode_metrics["NameNodeInfo"]["SoftwareVersion"].add_metric(label, 0)
            elif "Safemode" in metric and "Safemode" in bean:
                label = [self.cluster, self.target]
                self.hadoop_namenode_metrics["NameNodeInfo"]["Safemode"].add_metric(label, 0 if metric in bean and bean[metric] == "" else 1)
            else:
                label = [self.cluster, self.target]
                self.hadoop_namenode_metrics['NameNodeInfo'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

    def get_metrics(self, beans):
        for i in range(len(beans)):
            if 'NameNodeActivity' in beans[i]['name']:
                self.get_nnactivity_metrics(beans[i])
            if 'StartupProgress' in beans[i]['name']:
                self.get_startupprogress_metrics(beans[i])
            if 'FSNamesystem' in beans[i]['name'] and 'FSNamesystemState' not in beans[i]['name']:
                self.get_fsnamesystem_metrics(beans[i])
            if 'FSNamesystemState' in beans[i]['name']:
                self.get_fsnamesystem_state_metrics(beans[i])
            if 'RetryCache' in beans[i]['name']:
                self.get_retrycache_metrics(beans[i])
            if 'NameNodeInfo' in beans[i]['name']:
                self.get_nninfo_metrics(beans[i])
