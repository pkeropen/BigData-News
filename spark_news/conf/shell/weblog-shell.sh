#/bin/bash
echo "start log......"
#第一个参数是原日志文件，第二个参数是日志生成输出文件
java -jar /opt/jars/weblogs.jar /opt/datas/weblog.log /opt/datas/weblog-flume.log