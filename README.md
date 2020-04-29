# hadoop_jmx_exporter

Hadoop HDFS & YARN jmx metrics prometheus exporter.

# 运行

``` bash
➜  hadoop_jmx_exporter git:(master) ✗ pip2 install -r requirements.txt

➜  hadoop_exporter git:(master) ✗ python2 hadoop_jmx_exporter.py --help
usage: hadoop_jmx_exporter.py [-h] -cluster cluster_name
                              [-queue yarn_queue_regexp]
                              [-nns [namenode_jmx_url [namenode_jmx_url ...]]]
                              [-dns [datanode_jmx_url [datanode_jmx_url ...]]]
                              [-rms [resourcemanager_jmx_url [resourcemanager_jmx_url ...]]]
                              [-nms [nodemanager_jmx_url [nodemanager_jmx_url ...]]]
                              [-jns [journalnode_jmx_url [journalnode_jmx_url ...]]]
                              [-host host] [-port port]

hadoop jmx metric prometheus exporter

optional arguments:
  -h, --help            show this help message and exit
  -cluster cluster_name
                        Hadoop cluster name (maybe HA name)
  -queue yarn_queue_regexp
                        Regular expression of queue name. default: root.*
  -nns [namenode_jmx_url [namenode_jmx_url ...]]
                        Hadoop hdfs namenode jmx metrics URL.
  -dns [datanode_jmx_url [datanode_jmx_url ...]]
                        Hadoop datanode jmx metrics URL.
  -rms [resourcemanager_jmx_url [resourcemanager_jmx_url ...]]
                        Hadoop resourcemanager metrics jmx URL.
  -nms [nodemanager_jmx_url [nodemanager_jmx_url ...]]
                        Hadoop nodemanager jmx metrics URL.
  -jns [journalnode_jmx_url [journalnode_jmx_url ...]]
                        Hadoop journalnode jmx metrics URL.
  -host host            Listen on this address. default: 0.0.0.0
  -port port            Listen to this port. default: 6688
➜  hadoop_exporter git:(master) ✗

➜  hadoop_exporter git:(master) ✗ python2 hadoop_jmx_exporter.py -cluster yh-cdh -nns http://10.193.40.10:50070/jmx http://10.193.40.3:50070/jmx -dns http://10.193.40.9:50075/jmx  http://10.193.40.3:50075/jmx http://10.193.40.10:50075/jmx -rms http://yh-shhd-cdh04:8088/jmx http://yh-shhd-cdh01:8088/jmx -nms http://yh-shhd-cdh04:8042/jmx http://yh-shhd-cdh05:8042/jmx
Listen at 0.0.0.0:6688
```

浏览器打开 `http://127.0.0.1:6688/metrics` 查看 metrics。

# Bugs

如果有 Application 在运行的情况下重启 NodeManager，可能会导致 jmx 返回
的结果中，分配的 CPU 和 Memory 为负值，这是个 Bugs，issus：https://issues.apache.org/jira/browse/YARN-6966

``` code
/jmx?qry=Hadoop:service=NodeManager,name=NodeManagerMetrics

"AllocatedGB": -35,
"AllocatedContainers": -5,
"AvailableGB": 276,
"AllocatedVCores": -9,
```

# 参考

1. http://hadoop.apache.org/docs/r2.7.3/hadoop-project-dist/hadoop-common/Metrics.html#namenode
2. https://docs.cloudera.com/HDPDocuments/Ambari-2.7.5.0/using-ambari-core-services/content/amb_hdfs_users.html
