# hadoop_jmx_exporter

Hadoop HDFS & YARN metrics exporter.

# 运行

``` bash
➜  hadoop_jmx_exporter git:(master) ✗ pip install -r requirements.txt
➜  hadoop_exporter git:(master) ✗ python hadoop_jmx_exporter.py --help
usage: hadoop_jmx_exporter.py [-h] [-c cluster_name] [-hdfs namenode_jmx_url]
                              [-rm resourcemanager_jmx_url]
                              [-dn datanode_jmx_url] [-jn journalnode_jmx_url]
                              [-nm nodemanager_jmx_url] [-p metrics_path]
                              [-host ip_or_hostname] [-P port]

hadoop jmx metric prometheus exporter

optional arguments:
  -h, --help            show this help message and exit
  -c cluster_name, --cluster cluster_name
                        Hadoop cluster labels. (default "yh-cdh")
  -hdfs namenode_jmx_url, --namenode-url namenode_jmx_url
                        Hadoop hdfs metrics URL.
  -rm resourcemanager_jmx_url, --resourcemanager-url resourcemanager_jmx_url
                        Hadoop resourcemanager metrics URL.
  -dn datanode_jmx_url, --datanode-url datanode_jmx_url
                        Hadoop datanode metrics URL.
  -jn journalnode_jmx_url, --journalnode-url journalnode_jmx_url
                        Hadoop journalnode metrics URL.
  -nm nodemanager_jmx_url, --nodemanager-url nodemanager_jmx_url
                        Hadoop nodemanager metrics URL.
  -p metrics_path, --path metrics_path
                        Path under which to expose metrics. (default
                        "/metrics")
  -host ip_or_hostname, -ip ip_or_hostname, --address ip_or_hostname, --addr ip_or_hostname
                        Polling server on this address. (default "0.0.0.0")
  -P port, --port port  Listen to this port. (default "6688")

➜  hadoop_jmx_exporter git:(master) ✗ python hadoop_jmx_exporter.py -c yh-cdh -hdfs http://10.193.40.10:50070/jmx  -rm http://10.193.40.2:8088/jmx -dn http://10.193.40.9:50075/jmx -jn http://yh-shhd-cdh05:8480/jmx -nm http://yh-shhd-cdh05:8042/jmx
Listen at 0.0.0.0:6688
hang
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
