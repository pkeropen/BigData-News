## Structured Streaming + Kafka 集成 + Redis管理Offset（Kafka broker version 0.10.0 or high）

Google一下发现 Structured Streaming + Kafka集成，记录Offset的文章挺少的，我自己摸索一下，写了一个DEMO。[Github地址](https://github.com/pkeropen/BigData-News/tree/master/structured-streaming-demo)

#### 1. 准备


配置起始和结束的offset值（默认）

Schema信息
读取后的数据的Schema是固定的，包含的列如下：

Column | Type | 说明
------- | ------- | -------
key | binary | 信息的key
value | binary | 信息的value(我们自己的数据)
topic | string | 主题
partition | int | 分区
offset | long | 偏移值
timestamp | long | 时间戳
timestampType | int | 类型

example：

```
val df = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1,topic2")
  .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
  .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
  .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]
```

批处理还是流式处理都必须为Kafka Source设置如下选项：

选项 | 值 | 意义
------- | ------- | -------
assign | json值{"topicA":[0,1],"topicB":[2,4]} | 指定消费的TopicPartition，Kafka Source只能指定"assign","subscribe","subscribePattern"选项中的一个
subscribe | 一个以逗号隔开的topic列表 | 订阅的topic列表，Kafka Source只能指定"assign","subscribe","subscribePattern"选项中的一个
subscribePattern | Java正则表达式 | 订阅的topic列表的正则式，Kafka Source只能指定"assign","subscribe","subscribePattern"选项中的一个
kafka.bootstrap.servers | 以逗号隔开的host:port列表 | Kafka的"bootstrap.servers"配置

startingOffsets 与 endingOffsets 说明：
选项 | 值 | 默认值 | 支持的查询类型 | 意义
------- | ------- | ------- | ------- | -------  
startingOffsets | "earliest","lates"(仅streaming支持)；或者json 字符"""{"topicA":{"0":23,"1":-1},"TopicB":{"0":-2}}""" | 对于流式处理来说是"latest",对于批处理来说是"earliest" | streaming和batch | 查询开始的位置可以是"earliest"(从最早的位置开始),"latest"(从最新的位置开始),或者通过一个json为每个TopicPartition指定开始的offset。通过Json指定的话，json中-2可以用于表示earliest，-1可以用于表示latest。注意：对于批处理而言，latest值不允许使用的。
endingOffsets | latest or json string{"topicA":{"0":23,"1":-1},"topicB":{"0":-1}} | latest | batch | 一个批查询的结束位置，可以是"latest"，即最近的offset，或者通过json来为每个TopicPartition指定一个结束位置，在json中，-1表示latest，而-2是不允许使用的

#### 2. 主要代码

```
/**
  * StructuredStreaming
  * 记录kafka上一次的Offset，从之前的Offset继续消费
  */
object StructuredStreamingOffset {

  val LOGGER: Logger = LogManager.getLogger("StructuredStreamingOffset")

  //topic
  val SUBSCRIBE = "log"
  
  case class readLogs(context: String, offset: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("StructuredStreamingOffset")
      .getOrCreate()

    //开始 offset
    var startOffset = -1

    //init
    val redisSingle: RedisSingle = new RedisSingle()
    redisSingle.init(Constants.IP, Constants.PORT)
    //get redis
    if (redisSingle.exists(Constants.REDIDS_KEY) && redisSingle.getTime(Constants.REDIDS_KEY) != -1) {
      startOffset = redisSingle.get(Constants.REDIDS_KEY).toInt
    }

    //sink
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", SUBSCRIBE)
      .option("startingOffsets", "{\"" + SUBSCRIBE + "\":{\"0\":" + startOffset + "}}")
      .load()

    import spark.implicits._

    //row 包含: key、value 、topic、 partition、offset、timestamp、timestampType
    val lines = df.selectExpr("CAST(value AS STRING)", "CAST(offset AS LONG)").as[(String, Long)]

    val content = lines.map(x => readLogs(x._1, x._2.toString))

    val count = content.toDF("context", "offset")

    //sink foreach 记录offset
    val query = count
      .writeStream
      .foreach(new RedisWriteKafkaOffset)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
```

Spark Structured streaming API支持的输出源有：Console、Memory、File和Foreach, 这里用到Foreach，用Redis存储value

```
class RedisWriteKafkaOffset extends ForeachWriter[Row] {
  var redisSingle: RedisSingle = _

  override def open(partitionId: Long, version: Long): Boolean = {
    redisSingle = new RedisSingle()
    redisSingle.init(Constants.IP, Constants.PORT)
    true
  }
  
  override def process(value: Row): Unit = {
    val offset = value.getAs[String]("offset")
    redisSingle.set(Constants.REDIDS_KEY, offset)
  }

  override def close(errorOrNull: Throwable): Unit = {
    redisSingle.getJedis().close()
    redisSingle.getPool().close()
  }
}
```

#### 3. 总结

Structured Streaming 是一个基于 Spark SQL 引擎的、可扩展的且支持容错的流处理引擎。你可以像表达静态数据上的批处理计算一样表达流计算。Spark SQL 引擎将随着流式数据的持续到达而持续运行，并不断更新结果。性能上，结构上比Spark Streaming的有一定优势。可以[参考文章](https://zhuanlan.zhihu.com/p/51883927). 以上内容都是个人整理出来，如果有错误或者反馈，可以与我联系，一起交流！