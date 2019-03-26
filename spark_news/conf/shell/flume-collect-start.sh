#/bin/bash
echo "flume-collect start ......"
sh /bin/flume-ng agent --conf conf -f /opt/conf/flume-collect-conf.properties -n a1 -Dflume.root.logger=INFO,console