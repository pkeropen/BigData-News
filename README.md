##  基于Spark2.2新闻网大数据实时系统项目

### 1. 项目代码是参考[基于Spark2.x新闻网大数据实时分析可视化系统项目](https://blog.csdn.net/u011254180/article/details/80172452) ，谢谢作者分享心得！

### 2.环境配置

##### 2.1 CDH-5.14.2 (安装步骤可参考[地址](https://blog.51cto.com/kaliarch/2122467))，关于版本是按实际操作， CDH的版本兼容性很好。
```
1. HBase
2. Hive
3. Flume
4. Kafka
5. HDFS + Yarn
6. Oozie
7. Hue
8. Spark2
9. Zookeeper
10.Mysql
```

##### 2.2 主机配置
```
1.Hadoop01, 4核16G , centos7.2
2.Hadoop02, 2核8G, centos7.2
3.Haddop03, 2核8G, centos7.2

```

#### 2.3 安装依赖包
```
# yum  -y  install psmisc MySQL-python at bc bind-libs bind-utils cups-client cups-libs cyrus-sasl-gssapi cyrus-sasl-plain ed fuse fuse-libs httpd httpd-tools keyutils-libs-devel krb5-devel libcom_err-devel libselinux-devel libsepol-devel libverto-devel mailcap noarch mailx mod_ssl openssl-devel pcre-devel postgresql-libs python-psycopg2 redhat-lsb-core redhat-lsb-submod-security  x86_64 spax time zlib-devel wget psmisc
# chmod +x /etc/rc.d/rc.local
# echo "echo 0 > /proc/sys/vm/swappiness" >>/etc/rc.d/rc.local
# echo "echo never > /sys/kernel/mm/transparent_hugepage/defrag" >>/etc/rc.d/rc.local
# echo 0 > /proc/sys/vm/swappiness
# echo never > /sys/kernel/mm/transparent_hugepage/defrag
# yum -y install rpcbind
# systemctl start rpcbind
# echo "systemctl start rpcbind" >> /etc/rc.d/rc.local


安装perl支持

yum install perl* (yum安装perl相关支持)
yum install cpan (perl需要的程序库，需要cpan的支持，详细自行百度)
```


### 3.  编写数据生成模拟程序

##### 3.1 模拟从nginx生成日志的log，数据来源（搜狗实验室[下载](https://www.sogou.com/labs/resource/q.php)用户查询日志，搜索引擎查询日志库设计为包括约1个月(2008年6月)Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。）

##### 3.2 数据清洗
 
 ##### 数据格式为:访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
 
1. 将文件中的tab更换成逗号
 ```
cat weblog.log|tr "\t" "," > weblog2.log
```

2. 将文件中的空格更换成逗号
```
cat weblog2.log|tr " " "," > weblog.log
```

##### 3.3 主要代码段

```
  public static void readFileByLines(String fileName) {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        String tempString = null;

        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            fis = new FileInputStream(fileName);
            //// 从文件系统中的某个文件中获取字节
            isr = new InputStreamReader(fis, "GBK");
            br = new BufferedReader(isr);
            int count = 0;
            while ((tempString = br.readLine()) != null) {
                count++;
                //显示行号
                Thread.sleep(300);
                String str = new String(tempString.getBytes("GBK"), "UTF8");
                System.out.println("row:"+count+">>>>>>>>"+str);
                writeFile(writeFileName, str);
            }
            isr.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException e1) {
                }
            }
        }
    }
```

#### 3.4 打包成weblogs.jar,[打包步骤](https://blog.csdn.net/xuemengrui12/article/details/74984731),  写Shell脚本weblog-shell.sh

```
#/bin/bash
echo "start log......"
#第一个参数是原日志文件，第二个参数是日志生成输出文件
java -jar /opt/jars/weblogs.jar /opt/datas/weblog.log /opt/datas/weblog-flume.log

```    

#### 3.5 修改weblog-shell.sh可执行权限
```
chmod 777 weblog-shell.sh
```

### 4.  Flume数据采集配置

##### 4.1 将hadoop02, hadoop03中Flume数据采集到hadoop01中，而且hadoop02和hadoop03的flume配置文件大致相同

```
flume-collect-conf.properties

# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type =exec
a1.sources.r1.command= tail -F /opt/datas/weblog-flume.log

# Describe the sink
a1.sinks.k1.type = avro
a1.sinks.k1.hostname = hadoop01
a1.sinks.k1.port = 5555

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 1000
a1.channels.c1.keep-alive = 5

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1

```

##### 4.2 hadoop01通过flume接收hadoop02与hadoop03中flume传来的数据，并将其分别发送至hbase与kafka中，配置内容如下:

```
a1.sources = r1
a1.channels = kafkaC hbaseC
a1.sinks = kafkaSink hbaseSink


a1.sources.r1.type = avro
a1.sources.r1.channels = hbaseC kafkaC
a1.sources.r1.bind = hadoop01
a1.sources.r1.port = 5555 
a1.sources.r1.threads = 5 

#****************************flume + hbase******************************
a1.channels.hbaseC.type = memory
a1.channels.hbaseC.capacity = 10000
a1.channels.hbaseC.transactionCapacity = 10000
a1.channels.hbaseC.keep-alive = 20

a1.sinks.hbaseSink.type = asynchbase
## HBase表名
a1.sinks.hbaseSink.table = weblogs
## HBase表的列族名称
a1.sinks.hbaseSink.columnFamily = info
## 自定义异步写入Hbase
a1.sinks.hbaseSink.serializer = main.hbase.KfkAsyncHbaseEventSerializer
a1.sinks.hbaseSink.channel = hbaseC
## Hbase表的列 名称
a1.sinks.hbaseSink.serializer.payloadColumn = datetime,userid,searchname,retorder,cliorder,cliurl

#****************************flume + kafka******************************
a1.channels.kafkaC.type = memory
a1.channels.kafkaC.capacity = 10000
a1.channels.kafkaC.transactionCapacity = 10000
a1.channels.kafkaC.keep-alive = 20

a1.sinks.kafkaSink.channel = kafkaC
a1.sinks.kafkaSink.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.kafkaSink.brokerList = hadoop01:9092
a1.sinks.kafkaSink.topic = webCount
a1.sinks.kafkaSink.zookeeperConnect = hadoop01:2181
a1.sinks.kafkaSink.requiredAcks = 1
a1.sinks.kafkaSink.batchSize = 1
a1.sinks.kafkaSink.serializer.class = kafka.serializer.StringEncoder

```

##### 4.3  配置Flume执行Shell脚本

```
flume-collect-start.sh 分发到hadoop02，hadoop03 ,/opt/shell/

#/bin/bash
echo "flume-collect start ......"
sh /bin/flume-ng agent --conf conf -f /opt/conf/flume-collect-conf.properties -n a1 -Dflume.root.logger=INFO,console

```

```
flume-kfk-hb-start.sh 分发到hadoop01 ,/opt/shell

#/bin/bash
echo "flume-collect start ......"
sh /bin/flume-ng agent --conf conf -f /opt/conf/flume-hbase-kafka-conf.properties -n a1 -Dflume.root.logger=INFO,console

```

##### 4.4  Flume分发到Hbase集成

下载Flume源码并导入IDEA开发工具

    1）将apache-flume-1.7.0-src.tar.gz源码下载到本地解压
    2）通过IDEA导入flume源码
    3）根据flume-ng-hbase-sink模块源码修改
    4）修改代码SimpleAsyncHbaseEventSerializer.java
    5）具体代码看源码

```
KfkAsyncHbaseEventSerializer.java  关键代码

 @Override
    public List<PutRequest> getActions() {
        List<PutRequest> actions = new ArrayList<PutRequest>();
        if (payloadColumn != null) {
            byte[] rowKey;
            try {
                /*---------------------------代码修改开始---------------------------------*/
                // 解析列字段
                String[] columns = new String(this.payloadColumn).split(",");
                // 解析flume采集过来的每行的值
                String[] values = new String(this.payload).split(",");

                for (int i = 0; i < columns.length; i++) {
                    byte[] colColumn = columns[i].getBytes();
                    byte[] colValue = values[i].getBytes(Charsets.UTF_8);

                    // 数据校验：字段和值是否对应
                    if (columns.length != values.length) break;

                    // 时间
                    String datetime = values[0].toString();
                    // 用户id
                    String userid = values[1].toString();
                    // 根据业务自定义Rowkey
                    rowKey = SimpleRowKeyGenerator.getKfkRowKey(userid, datetime);
                    // 插入数据
                    PutRequest putRequest = new PutRequest(table, rowKey, cf,
                            colColumn, colValue);
                    actions.add(putRequest);
                    /*---------------------------代码修改结束---------------------------------*/
                }
            } catch (Exception e) {
                throw new FlumeException("Could not get row key!", e);
            }
        }
        return actions;
    }


```

##### 4.5  将项目打包成jar，vita-flume-ng-hbase-sink.jar,分发到CDH的Flume/libs/下



### 5.  Kafka配置(测试环境，Kafka部署hadoop01,不做高可用)

##### 5.1 配置

配置advertised.listeners：=PLAINTEXT://xxxx:9092

##### 5.2 测试生产消费是否成功

```
//create topic,副本数为1、分区数为1的topic,如果是配置了auto.create.topics.enable参数为true，可以忽略
sh bin/kafka-topics.sh --create --zookeeper hadoop01:2181 --topic webCount --replication-factor 1 --partitions 1

//producer
sh /bin/kafka-console-producer --broker-list hadoop01:9092 --topic webCount

//consumer
sh /bin/kafka-console-consumer --zookeeper hadoop01:2181 --topic webCount --from-beginning

//delete topic
sh /bin/kafka-topics  --delete --zookeeper hadoop01  --topic webCount

//topic list
sh /bin/kafka-topics --zookeeper hadoop01:2181 --list

```


##### 5.3  编写Kafka Consumer执行脚本kfk-test-consumer.sh,分发到/opt/shell/

```
#/bin/bash
echo "kfk-kafka-consumer.sh start......"
/bin/kafka-console-consumer --zookeeper hadoop01:2181 --from-beginning --topic webCount

```


### 6.  Hbase配置

##### 6.1 创建业务表

```
create 'weblogs','info'

//查看数据
count 'weblogs'

```

### 7. Hive配置

##### 7.1 CDH配置 Hive与Hbase集成，或者配置
```
<property>
    <name>hbase.zookeeper.quorum</name>   
    <value>hadoop01,hadoop02,hadoop03</value>
</property>

```

##### 7.2 在hive中创建与hbase集成的外部表
```
CREATE EXTERNAL TABLE weblogs(
id string,
datetime string,
userid string,
searchname string,
retorder string,
cliorder string,
cliurl string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES("hbase.columns.mapping"=
":key,info:datetime,info:userid,info:searchname,info:retorder,info:cliorder,info:cliurl")
TBLPROPERTIES("hbase.table.name"="weblogs");
 
#查看hbase数据记录
select count(*) from weblogs;

# 查看表
show tables;

# 查看前10条数据
select * from weblogs limit 10;

```

### 8. Structured Streaming配置

##### 8.1 测试Spark与mysql

```
   val df =spark.sql("select count(1) from weblogs").show
```

##### 8.2 Structured Streaming与MySQL集成

mysql创建相应的数据库和数据表，用于接收数据

```
create database test;
use test;
 
CREATE TABLE `webCount` (
    `titleName` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
    `count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

```

##### 8.3 Structured Streaming关键代码
```
/**
  * 结构化流从kafka中读取数据存储到关系型数据库mysql
  * 目前结构化流对kafka的要求版本0.10及以上
  */
object StructuredStreamingKafka {

  case class Weblog(datatime: String,
                    userid: String,
                    searchname: String,
                    retorder: String,
                    cliorder: String,
                    cliurl: String)

  val LOGGER: Logger = LogManager.getLogger("vita")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("streaming")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop01:9092")
      .option("subscribe", "webCount")
      .load()

    import spark.implicits._

    val lines = df.selectExpr("CAST(value AS STRING)").as[String]
    //    //    lines.map(_.split(",")).foreach(x => print(" 0 = " + x(0) + " 1 = " + x(1) + " 2 = " + x(2) + " 3 = " + x(3) + " 4 = " + x(4) + " 5 = " + x(5)))

    val weblog = lines.map(_.split(","))
      .map(x => Weblog(x(0), x(1), x(2), x(3), x(4), x(5)))

    val titleCount = weblog
      .groupBy("searchname")
      .count()
      .toDF("titleName", "count")

    val url = "jdbc:mysql://hadoop01:3306/test"
    val username = "root"
    val password = "root"

    val writer = new JDBCSink(url, username, password)
    //        val writer = new MysqlSink(url, username, password)

    val query = titleCount
      .writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }
}

```

##### 8.4 项目打包，spark-weblogs.jar.

### 9. 启动流程

##### 9.1 CDH启动Zookeeper，Hadoop，Hbase，Mysql，Yarn，Flume，Kafka

##### 9.2 先在Hadoop01 执行/opt/shell/flume-kfk-hb-start.sh 将数据分别传到hbase和kafka中

##### 9.3 在Hadoop02，Hadoop03 执行/opt/shell/flume-collect-start.sh  将数据发送到Hadoop01中

##### 9.4 在hadoop01 , 执行提交Spark任务

```
spark on yarn, 集成spark-sql-kafka

sh /bin/spark2-submit \
  --class com.vita.spark.StructuredStreamingKafka \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 1G \
  --executor-cores 2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 \
  /opt/jars/spark-weblogs.jar \
  10
```

用IDEA 远程调试Spark代码，参考[地址](https://blog.csdn.net/yiluohan0307/article/details/80048765)
```
sh /bin/spark2-submit \
  --class com.vita.spark.StructuredStreamingKafka \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 1G \
  --executor-cores 1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 \
  --driver-java-options "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005" \
  /opt/jars/spark-weblogs.jar \
  10
```

* Yarn kill Spark任务 ： yarn application -kill [任务名]

##### 9.5 在Hadoop02，Hadoop03 执行/opt/weblog-shell.sh , 启动 StructuredStreamingKafka来从kafka中取得数据，处理后存到mysql中

##### 9.6 登录mysql ，查看数据表


